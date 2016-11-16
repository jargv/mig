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

	filename, packagename := callerInfo()

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

	recorded, err := fetchRecordedSteps(db, pkgs)
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

func skipCompletedSteps(steps []Step, recorded map[string]recordedStep) []Step {
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

func doRecordedReverts(db DB, reverts map[string]recordedStep) error {
	// TODO: make this transactional

	if forward_only && len(reverts) > 0 {
		hashes := []string{}
		for _, revert := range reverts {
			hashes = append(hashes, revert.Hash)
		}
		return fmt.Errorf("refusing to do reverts when running in forward-only mode\n Revert Hashes: %s", hashes)
	}

	orderedSteps := orderRecordedReverts(reverts)

	for _, step := range orderedSteps {
		stmt := fmt.Sprintf(`
			DELETE FROM MIG_RECORDED_MIGRATIONS
			WHERE       hash = %s
		`, arg(db, 1))
		_, err := db.Exec(stmt, step.Hash)
		if err != nil {
			return fmt.Errorf(
				"coudln't delete from MIG_RECORDED_MIGRATIONS table (hash '%s'): %v",
				step.Hash, err,
			)
		}

		//nothing to do if the revert is the empty string
		if step.Revert == "" {
			continue
		}

		_, err = db.Exec(step.Revert)
		if err != nil {
			return fmt.Errorf(
				"coudln't execute recorded revert '%s': %v",
				step.Revert, err,
			)
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
			_ = tx.Rollback()
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
		_, err = tx.Exec(stmt, step.Name, step.file, step.hash, step.pkg, step.revert(), now)
		if err != nil {
			_ = tx.Rollback()
			return nil, false, fmt.Errorf(
				"internal mig error (couldn't insert into MIG_RECORDED_MIGRATIONS table): %v", err,
			)
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return nil, false, fmt.Errorf("couldn't commit transaction: %v", err)
		}

		progress = true
	}

	return steps, progress, nil
}

func checkMigrationTable(db DB) error {
	_, err := db.Query(`select 1 from MIG_RECORDED_MIGRATIONS`)
	if err != nil {
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
		if err != nil {
			return fmt.Errorf("creating MIG_RECORDED_MIGRATIONS table: %v", err)
		}
	}

	_, err = db.Query(`select migration_order from MIG_RECORDED_MIGRATIONS limit 0`)
	if err != nil {
		var columnType string
		if db.DriverName() == "mysql" {
			columnType = "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE"
		} else if db.DriverName() == "postgres" {
			columnType = "BIGSERIAL"
		} else {
			panic(fmt.Errorf("unsupported driver named %s", db.DriverName()))
		}
		_, err := db.Exec(fmt.Sprintf(`
		  ALTER TABLE MIG_RECORDED_MIGRATIONS
			ADD COLUMN  migration_order %s
		`, columnType))
		if err != nil {
			return fmt.Errorf("adding order column: %v", err)
		}
	}

	return nil
}

func fetchRecordedSteps(db DB, pkgs []string) (map[string]recordedStep, error) {
	//collect the recored migrations
	stmt := fmt.Sprintf(`
		SELECT hash, revert, migration_order
		FROM   MIG_RECORDED_MIGRATIONS
		WHERE  pkg in (%s)
	`, "'"+strings.Join(pkgs, "','")+"'")
	rows, err := db.Query(stmt)
	if err != nil {
		return nil, err
	}

	recorded := map[string]recordedStep{}
	for rows.Next() {
		var step recordedStep
		_ = rows.Scan(&step.Hash, &step.Revert, &step.Order)
		recorded[step.Hash] = step
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

func callerInfo() (string, string) {
	pc, filename, _, ok := runtime.Caller(2)
	if !ok {
		panic(fmt.Errorf("couldn't use runtime to collect mig.Register info"))
	}

	// callername looks like "github.com/jargv/mig.Run"
	// unless it is a method, then it looks like "like github.com/jargv/mig.(*Type).Foo"
	callerName := runtime.FuncForPC(pc).Name()
	parts := strings.Split(callerName, ".")
	nParts := len(parts)
	var packagename string
	if nParts > 2 && len(parts[nParts-2]) > 0 && parts[nParts-2][0] == '(' {
		packagename = strings.Join(parts[:len(parts)-2], ".")
	} else {
		packagename = strings.Join(parts[:len(parts)-1], ".")
	}
	return filename, packagename
}
