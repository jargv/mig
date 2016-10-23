package mig

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

var registeredMigrationSets [][]Step
var taggedMigrationSets map[string][][]Step

// Register queues a MigrationSet to be exectued when mig.Run(...) is called
func Register(steps_in []Step, tags ...string) {
	// deep copy to avoid reference issue
	steps := make([]Step, len(steps_in))
	for i, step := range steps_in {
		step.cleanWhitespace()
		step.computeHash()
		steps[i] = step
	}

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
		return fmt.Errorf("couldn't create migration table: %v", err)
	}

	// copy so that the operations are not mutating
	sets := make([][]Step, len(steps))
	copy(sets, steps)

	recorded, err := fetchCompletedSteps(db)
	if err != nil {
		return fmt.Errorf("couldn't fetch previous migrations: %v", err)
	}

	for i, set := range sets {
		sets[i] = skipCompletedSteps(set, recorded)
	}

	err = doRecordedReverts(db, recorded)
	if err != nil {
		return fmt.Errorf("couldn't perform recorded reverts: %v", err)
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
		hash := steps[0].hash
		if _, ok := recorded[hash]; !ok {
			break //we've found the migration to start at. Everything else must be redone.
		}
		delete(recorded, hash)
		steps = steps[1:]
	}

	return steps
}

func doRecordedReverts(db *sqlx.DB, reverts map[string]string) error {
	// TODO: do this in reverse chronology!
	for hash, revert := range reverts {
		stmt := fmt.Sprintf(`
			DELETE from migration
			WHERE  hash = %s
		`, arg(db, 1))
		_, err := db.Exec(stmt, hash)
		if err != nil {
			return fmt.Errorf("coudln't delete from migration table (hash '%s'): %v", hash, err)
		}

		//nothing to do if the revert is the empty string
		if revert == "" {
			continue
		}

		_, err = db.Exec(revert)
		if err != nil {
			return fmt.Errorf("coudln't execute recorded revert '%s': %v", revert, err)
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
			return nil, false, fmt.Errorf(
				"couldn't execute migration '%s': %v",
				step.Migrate, err,
			)
		}

		stmt := fmt.Sprintf(`
			INSERT into migration (hash, revert)
			VALUES (%s, %s);
		`, arg(db, 1), arg(db, 2))
		_, err = tx.Exec(stmt, step.hash, step.Revert)
		if err != nil {
			tx.Rollback()
			return nil, false, fmt.Errorf(
				"internal mig error (couldn't insert into migration table): %v", err,
			)
		}

		err = tx.Commit()
		if err != nil {
			return nil, false, fmt.Errorf("couldn't commit transaction: %v", err)
		}

		progress = true
	}

	return steps, progress, nil
}

func checkMigrationTable(db *sqlx.DB) error {
	_, err := db.Query(`select 1 from migration`)
	if err == nil {
		return nil //it already exists
	}

	//TODO: timestamps and other audit trails
	//TODO: pick a better name for the table
	_, err = db.Exec(`
		CREATE TABLE migration (
			hash TEXT,
			revert TEXT
		)
	`)

	return err
}

func fetchCompletedSteps(db *sqlx.DB) (map[string]string, error) {
	//collect the recored migrations
	rows, err := db.Query(`
		SELECT hash, revert
		FROM   migration
	`)
	if err != nil {
		return nil, err
	}

	recorded := map[string]string{}
	for rows.Next() {
		var hash, revert string
		rows.Scan(&hash, &revert)
		recorded[hash] = revert
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return recorded, nil
}

func arg(db *sqlx.DB, n int) string {
	switch driver := db.DriverName(); driver {
	case "mysql":
		return "?"
	case "postgres":
		return fmt.Sprintf("$%d", n)
	default:
		panic(fmt.Sprintf("mig doesn't support db connections with driver '%s'", driver))
	}
}
