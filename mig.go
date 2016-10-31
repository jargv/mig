package mig

import (
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"time"
)

// TODO: support for logging
// TODO: better readme, docs
// TODO: build tag for mig_forward_only
// TODO: consider a method for creating a new baseline. For example, keep
//       the data in an existing installment, but pretend like it's
//       migrations came from the new refactored versions of the migrations.

// DB is an interface that allows you to use the standard *sql.DB or a *sqlx.DB
// (or any other connection that implements the interface!)
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Begin() (*sql.Tx, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	DriverName() string
}

var registeredMigrations map[string][][]Step

// Register queues a MigrationSet to be exectued when mig.Run(...) is called
func Register(stepsIn []Step) {
	// get the file name of the calling function

	pc, filename, _, ok := runtime.Caller(1)
	if !ok {
		panic(fmt.Errorf("couldn't use runtime to collect mig.Register info"))
	}

	//callername looks like "github.com/jargv/mig.Run"
	callerName := runtime.FuncForPC(pc).Name()
	parts := strings.Split(callerName, ".")
	packagename := strings.Join(parts[:len(parts)-1], ".")

	// deep copy to avoid reference issues
	steps := make([]Step, len(stepsIn))
	for i, step := range stepsIn {
		step.cleanWhitespace()
		step.computeHash()
		step.file = filename
		step.pkg = packagename
		if step.Name == "" {
			step.Name = fmt.Sprintf("unnamed-%d", i)
		}
		steps[i] = step
	}

	if registeredMigrations == nil {
		registeredMigrations = make(map[string][][]Step)
	}

	registeredMigrations[packagename] = append(registeredMigrations[packagename], steps)
}

// Run executes the migration Steps which have been registered by `mig.Register`
// on the given database connection
func Run(db DB) error {
	err := checkMigrationTable(db)
	if err != nil {
		return fmt.Errorf("creating migration table: %v", err)
	}

	pkgs := []string{}
	sets := [][]Step{}
	for pkg, setsForpkg := range registeredMigrations {
		pkgs = append(pkgs, pkg)
		sets = append(sets, setsForpkg...)
	}

	recorded, err := fetchCompletedSteps(db, pkgs)
	if err != nil {
		return fmt.Errorf("fetching previous migrations: %v", err)
	}

	for i, set := range sets {
		sets[i] = skipCompletedSteps(set, recorded)
	}

	err = doRecordedReverts(db, recorded)
	if err != nil {
		return fmt.Errorf("performing recorded reverts: %v", err)
	}

	return runSteps(db, sets)
}

func runSteps(db DB, sets [][]Step) error {
	//run the migration sets
	for {
		morePending := false
		progressMade := false

		for i, set := range sets {
			newSet, currentSetProgressed, err := tryProgressOnSet(db, set)
			if err != nil {
				return err
			}

			// track if any progress is made
			if currentSetProgressed {
				progressMade = true
			}

			// track if more work is pending
			if len(newSet) > 0 {
				morePending = true
			}

			sets[i] = newSet
		}

		//if there isn't any more work, we're done!
		if !morePending {
			break
		}

		//if no progress was made, we're in an infinite loop
		if !progressMade {
			return fmt.Errorf("Unable to make progress on migration steps: %#v", sets)
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

func doRecordedReverts(db DB, reverts map[string]string) error {
	// TODO: do this in reverse chronology!
	// TODO: make this transactional
	for hash, revert := range reverts {
		stmt := fmt.Sprintf(`
			DELETE FROM MIG_RECORDED_MIGRATIONS
			WHERE       hash = %s
		`, arg(db, 1))
		_, err := db.Exec(stmt, hash)
		if err != nil {
			return fmt.Errorf("coudln't delete from MIG_RECORDED_MIGRATIONS table (hash '%s'): %v", hash, err)
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

func tryProgressOnSet(db DB, steps []Step) ([]Step, bool, error) {
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
			_ = tx.Rollback()
			return nil, false, fmt.Errorf(
				"couldn't execute migration '%s': %v\n"+
					"file: %s\n"+
					"sql: `%s`",
				step.Name, err, step.file, step.Migrate,
			)
		}

		now := time.Now()
		stmt := fmt.Sprintf(`
			INSERT into MIG_RECORDED_MIGRATIONS (name, file, hash, pkg, revert, time)
			VALUES (%s, %s, %s, %s, %s, %s);
		`, arg(db, 1), arg(db, 2), arg(db, 3), arg(db, 4), arg(db, 5), arg(db, 6))
		_, err = tx.Exec(stmt, step.Name, step.file, step.hash, step.pkg, step.Revert, now)
		if err != nil {
			_ = tx.Rollback()
			return nil, false, fmt.Errorf(
				"internal mig error (couldn't insert into MIG_RECORDED_MIGRATIONS table): %v", err,
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

func checkMigrationTable(db DB) error {
	_, err := db.Query(`select 1 from MIG_RECORDED_MIGRATIONS`)
	if err == nil {
		return nil //it already exists
	}

	_, err = db.Exec(`
		CREATE TABLE MIG_RECORDED_MIGRATIONS (
			name   TEXT,
			file   TEXT,
			pkg    TEXT,
			time   TIMESTAMP,
			hash   TEXT,
			revert TEXT
		)
	`)

	return err
}

func fetchCompletedSteps(db DB, pkgs []string) (map[string]string, error) {
	//collect the recored migrations
	stmt := fmt.Sprintf(`
		SELECT hash, revert
		FROM   MIG_RECORDED_MIGRATIONS
		WHERE  pkg in (%s)
	`, "'"+strings.Join(pkgs, "','")+"'")
	rows, err := db.Query(stmt)
	if err != nil {
		return nil, err
	}

	recorded := map[string]string{}
	for rows.Next() {
		var hash, revert string
		_ = rows.Scan(&hash, &revert)
		recorded[hash] = revert
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return recorded, nil
}

func arg(db DB, n int) string {
	switch driver := db.DriverName(); driver {
	case "mysql":
		return "?"
	case "postgres":
		return fmt.Sprintf("$%d", n)
	default:
		panic(fmt.Sprintf("mig doesn't support db connections with driver '%s'", driver))
	}
}
