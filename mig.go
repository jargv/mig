package mig

import (
	"database/sql"
	"fmt"
	"runtime"
	"strings"
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

var registered map[string][]*series

// Register queues a MigrationSet to be exectued when mig.Run(...) is called
func Register(steps []Step) {
	// get the file name of the calling function

	filename, packagename := callerInfo()

	ser := &series{}
	ser.steps = make([]Step, len(steps))
	for i := range steps {
		ser.steps[i] = steps[i]
		step := &ser.steps[i]
		step.cleanWhitespace()
		step.computeHash()
		step.file = filename
		step.pkg = packagename
		if step.Name == "" {
			step.Name = fmt.Sprintf("unnamed-%d", i)
		}
	}

	if registered == nil {
		registered = make(map[string][]*series)
	}

	registered[packagename] = append(registered[packagename], ser)
}

// Run executes the migration Steps which have been registered by `mig.Register`
// on the given database connection
func Run(db DB) error {
	err := checkMigrationTable(db)
	if err != nil {
		return fmt.Errorf("creating migration table: %v", err)
	}

	allPackages := []string{}
	allSeries := []*series{}
	for pkg, setsForpkg := range registered {
		allPackages = append(allPackages, pkg)
		allSeries = append(allSeries, setsForpkg...)
	}

	recorded, err := fetchRecordedSteps(db, allPackages)
	if err != nil {
		return fmt.Errorf("fetching previous migrations: %v", err)
	}

	for _, series := range allSeries {
		series.syncWithRecordedSteps(recorded)
	}

	revertPoint := -1
	for _, r := range recorded {
		if revertPoint == -1 || r.Order < revertPoint {
			revertPoint = r.Order
		}
	}

	if revertPoint != -1 {
		err = doRevertsAtRevertPoint(db, revertPoint)
		if err != nil {
			return fmt.Errorf("doing reverts: %v", err)
		}

		for _, series := range allSeries {
			series.rewindToRevertPoint(revertPoint)
		}
	}

	return runSteps(db, allSeries)
}

func runSteps(db DB, allSeries []*series) error {
	//run the migration sets
	for {
		morePending := false
		progressMade := false

		for _, series := range allSeries {
			currentSetProgressed, err := series.tryProgress(db)
			if err != nil {
				return err
			}

			// track if any progress is made
			if currentSetProgressed {
				progressMade = true
			}

			// track if more work is pending
			if !series.done() {
				morePending = true
			}
		}

		//if there isn't any more work, we're done!
		if !morePending {
			break
		}

		//if no progress was made, we're in an infinite loop
		if !progressMade {
			return fmt.Errorf("Unable to make progress on migration steps: %#v", allSeries)
		}
	}

	return nil
}

func doRevertsAtRevertPoint(db DB, revertPoint int) error {
	// TODO: make this transactional

	if forward_only {
		return fmt.Errorf(
			"refusing to do reverts when running in forward-only mode. Revert Point: %d",
			revertPoint,
		)
	}

	type revertStep struct {
		Hash, Revert string
	}

	revertSteps := []revertStep{}

	stmt := fmt.Sprintf(`
	  SELECT   revert, hash
		FROM     MIG_RECORDED_MIGRATIONS
		WHERE    migration_order >= %s
		ORDER BY migration_order DESC
	`, arg(db, 1))

	rows, err := db.Query(stmt, revertPoint)
	if err != nil {
		return fmt.Errorf("fetching recorded reverts: %v", err)
	}

	for rows.Next() {
		var step revertStep
		_ = rows.Scan(&step.Revert, &step.Hash)
		revertSteps = append(revertSteps, step)
	}

	if rows.Err() != nil {
		return fmt.Errorf("fetching reverts: %v", rows.Err())
	}

	for _, step := range revertSteps {
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
				"executing recorded revert '%s': %v",
				step.Revert, err,
			)
		}
	}

	return nil
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
		SELECT hash, migration_order
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
		_ = rows.Scan(&step.Hash, &step.Order)
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
