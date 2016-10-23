package mig

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"

	"github.com/jmoiron/sqlx"
)

var registeredMigrationSets [][]Step
var taggedMigrationSets map[string][][]Step

// Step represents a single step in a migration
type Step struct {
	Migrate string
	Revert  string
	Prereq  string
}

// Register queues a MigrationSet to be exectued when mig.Run(...) is called
func Register(steps []Step, tags ...string) {
	//copy to avoid refernce issues
	steps = append([]Step{}, steps...)

	if len(tags) == 0 {
		registeredMigrationSets = append(registeredMigrationSets, steps)
		return
	}

	if taggedMigrationSets == nil {
		taggedMigrationSets = make(map[string][][]Step)
	}

	for _, tag := range tags {
		taggedMigrationSets[tag] = append(taggedMigrationSets[tag], steps)
	}
}

// Run executes the migration Steps which have been registered by `mig.Register`
// on the given database connection
func Run(db *sqlx.DB, tags ...string) error {
	if len(tags) == 0 {
		return run(db, registeredMigrationSets)
	}

	for _, tag := range tags {
		err := run(db, taggedMigrationSets[tag])
		if err != nil {
			return err
		}
	}

	return nil
}

func run(db *sqlx.DB, steps [][]Step) error {
	err := checkMigrationTable(db)
	if err != nil {
		return err
	}

	// copy so that the operations are not mutating
	sets := make([][]Step, len(steps))
	copy(sets, steps)

	recorded, err := fetchCompletedSteps(db)
	if err != nil {
		return err
	}

	for i, set := range sets {
		sets[i] = skipCompletedSteps(set, recorded)
	}

	err = doRecordedReverts(db, recorded)
	if err != nil {
		return err
	}

	return runSteps(db, sets)
}

func runSteps(db *sqlx.DB, sets [][]Step) error {
	mostRecentProgress := -1

	//run the migration sets
	for {
		retry := false
		for i, set := range sets {
			newSet, progressMade, err := tryProgressOnSet(db, set)
			if err != nil {
				return err
			}

			// track the most recent progress to detect an infinite loop
			if progressMade {
				mostRecentProgress = i
			} else if mostRecentProgress == i && len(newSet) > 0 {
				// if the most recent progress was from this set,
				// but we didn't make progress this time,
				// we must be caught in a loop. Fail
				return fmt.Errorf("Unable to make progress on migration steps: %v", sets)
			}

			// if this set isn't done, we'll have to continue at least one more iteration
			if len(newSet) > 0 {
				retry = true
			}

			sets[i] = newSet
		}

		if !retry {
			break
		}
	}

	return nil
}

func skipCompletedSteps(steps []Step, recorded map[string]string) []Step {
	for len(steps) > 0 {
		hash := createHash(steps[0].Migrate)
		if _, ok := recorded[hash]; !ok {
			break //we've found the migration to start at. Everything else must be redone.
		}
		delete(recorded, hash)
		steps = steps[1:]
	}

	return steps
}

func doRecordedReverts(db *sqlx.DB, reverts map[string]string) error {
	// do the rewind process
	// TODO: do this in reverse chronology!
	for hash, down := range reverts {
		_, err := db.Exec(`
			DELETE from migration
			WHERE  hash = $1
		`, hash)
		if err != nil {
			return err
		}

		_, err = db.Exec(down)
		if err != nil {
			return err
		}
	}

	return nil
}

func tryProgressOnSet(db *sqlx.DB, steps []Step) ([]Step, bool, error) {
	progress := false

	for len(steps) > 0 {
		step := steps[0]

		if len(step.Prereq) > 0 {
			_, err := db.Exec(step.Prereq)
			if err != nil {
				return steps, progress, nil
			}
		}

		steps = steps[1:]

		tx, err := db.Begin()

		if err != nil {
			return nil, false, err
		}

		_, err = tx.Exec(step.Migrate)

		if err != nil {
			tx.Rollback()
			return nil, false, err
		}

		hash := createHash(step.Migrate)

		_, err = tx.Exec(`
			INSERT into migration (hash, down)
			VALUES ($1, $2);
		`, hash, step.Revert)

		if err != nil {
			tx.Rollback()
			return nil, false, err
		}

		err = tx.Commit()
		if err != nil {
			return nil, false, err
		}

		progress = true
	}

	return steps, progress, nil
}

func createHash(str string) string {
	//TODO: remove empty lines and leading whitespace to make this a bit more robust
	sum := md5.Sum([]byte(str))
	b64 := base64.StdEncoding.EncodeToString(sum[:])
	return string(b64[:])
}

func checkMigrationTable(db *sqlx.DB) error {
	_, err := db.Query(`select 1 from migration;`)
	if err == nil {
		return nil //it already exists
	}

	//TODO: timestamps and other audit trails
	_, err = db.Exec(`
		CREATE TABLE migration (
			hash TEXT,
			down TEXT
		)
	`)

	return err
}

func fetchCompletedSteps(db *sqlx.DB) (map[string]string, error) {
	//collect the recored migrations
	rows, err := db.Query(`
	SELECT hash, down
	FROM   migration
	`)
	if err != nil {
		return nil, err
	}

	recorded := map[string]string{}
	for rows.Next() {
		var hash, down string
		rows.Scan(&hash, &down)
		recorded[hash] = down
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return recorded, nil
}
