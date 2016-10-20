package mig

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"

	"github.com/jmoiron/sqlx"
)

var registeredMigrationSets [][]Step

// Step represents a single step in a migration
type Step struct {
	Migrate string
	Revert  string
	Prereq  string
}

// Register queues a MigrationSet to be exectued when mig.Run(...) is called
func Register(set []Step) {
	registeredMigrationSets = append(registeredMigrationSets, set)
}

func clearMigrationsForNextTest() {
	registeredMigrationSets = nil
}

// Run executes the migration Steps which have been registered by `mig.Register`
// on the given database connection
func Run(db *sqlx.DB) error {
	err := checkMigrationTable(db)
	if err != nil {
		return err
	}

	sets := make([][]Step, len(registeredMigrationSets))
	copy(sets, registeredMigrationSets)

	recorded, err := collectRecordedMigrations(db)
	if err != nil {
		return err
	}

	for i, set := range sets {
		sets[i] = rewindSet(set, recorded)
	}

	err = doReverts(db, recorded)
	if err != nil {
		return err
	}

	//todo: detect the case where there's no progress

	mostRecentProgress := -1

	for {
		retry := false
		for i, set := range sets {
			newSet, progressMade, err := progressSet(db, set)
			if err != nil {
				return err
			}

			if progressMade {
				mostRecentProgress = i
			} else if mostRecentProgress == i && len(newSet) > 0 {
				return fmt.Errorf("Unable to make progress on migration steps: %v", sets)
			}

			//if this set isn't done, we'll have to cycle back around
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

func rewindSet(steps []Step, recorded map[string]string) []Step {
	for len(steps) > 0 {
		hash := createHash(steps[0].Migrate)
		if _, ok := recorded[hash]; !ok {
			break //we've found the migration to rewind to!
		}
		delete(recorded, hash)
		steps = steps[1:]
	}

	return steps
}

func doReverts(db *sqlx.DB, reverts map[string]string) error {
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

func progressSet(db *sqlx.DB, steps []Step) ([]Step, bool, error) {
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

func collectRecordedMigrations(db *sqlx.DB) (map[string]string, error) {
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
