package mig

import (
	"database/sql"
	"fmt"
	"regexp"
	"runtime"
	"strings"
)

// TODO: support for logging
// TODO: better readme, docs
// TODO: clean up the implementation, there's still remnants from when
//       it was much more complicated

// DB is an interface that allows you to use the standard *sql.DB or a *sqlx.DB
// (or any other connection that implements the interface!)
// todo: make this actually work without sqlx
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Begin() (*sql.Tx, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	DriverName() string
}

type db struct {
	*sql.DB
	driverName string
}

func (d *db) DriverName() string {
	return d.driverName
}

func MakeDB(driver string, d *sql.DB) *db {
	return &db{d, driver}
}

type step struct {
	migrate string

	isPrereq bool

	hash  string
	file  string
	pkg   string
	order int
}

type Prereq string

var registered map[string][]*series

// Register queues a MigrationSet to be exectued when mig.Run(...) is called
func RegisterMigrations(steps ...interface{}) {
	// get the file name of the calling function
	filename, packagename := callerInfo()

	ser := &series{}
	ser.steps = make([]step, len(steps))
	for i := range steps {
		switch s := steps[i].(type) {
		case string:
			ser.steps[i] = step{
				migrate: s,
			}
		case Prereq:
			ser.steps[i] = step{
				migrate:  string(s),
				isPrereq: true,
			}
		}
		step := &ser.steps[i]
		step.cleanWhitespace()
		step.computeHash()
		step.file = filename
		step.pkg = packagename
	}

	if registered == nil {
		registered = make(map[string][]*series)
	}

	registered[packagename] = append(registered[packagename], ser)
}

// Run executes the migration Steps which have been registered by `mig.Register`
// on the given database connection
func RunMigrations(db DB) error {
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

	return runSteps(db, recorded, allSeries)
}

func runSteps(db DB, hashes map[string]struct{}, allSeries []*series) error {
	//run the migration sets
	for {
		morePending := false
		progressMade := false

		for _, series := range allSeries {
			currentSetProgressed, err := series.tryProgress(db, hashes)
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
			return &progressError{allSeries}
		}
	}

	return nil
}

func checkMigrationTable(db DB) error {
	_, err := db.Query(`select 1 from MIG_RECORDED_MIGRATIONS`)
	if err != nil {
		_, err = db.Exec(`
			CREATE TABLE MIG_RECORDED_MIGRATIONS(
				sql_text TEXT,
				file     TEXT,
				pkg      TEXT,
				time     TIMESTAMP,
				hash     TEXT
			)
		`)
		if err != nil {
			return fmt.Errorf("creating MIG_RECORDED_MIGRATIONS table: %v", err)
		}
	}

	return nil
}

func fetchRecordedSteps(db DB, pkgs []string) (map[string]struct{}, error) {
	//collect the recored migrations
	stmt := fmt.Sprintf(`
		SELECT hash
		FROM   MIG_RECORDED_MIGRATIONS
	`)
	rows, err := db.Query(stmt)
	if err != nil {
		return nil, err
	}

	recorded := map[string]struct{}{}
	for rows.Next() {
		var hash string
		_ = rows.Scan(&hash)
		recorded[hash] = struct{}{}
	}

	err = rows.Err()
	if err != nil {
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

	//init functions end up with .initN
	reg := regexp.MustCompile(`\.init\d*$`)
	packagename = reg.ReplaceAllString(packagename, "")

	return filename, packagename
}
