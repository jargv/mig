package mig

import (
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

/*mysql:
  requires mysqladmin
  create user 'testuser'@'localhost' identified by 'testpassword';
	grant all privileges on testdb . * to 'testuser'@'localhost';
*/

/*postgres:
  create user testuser;
	alter user testuser password 'testpassword'
	alter user testuser createdb;
*/

func Test(t *testing.T) {
	//create the postgres table
	_ = exec.Command("dropdb", "-U", "testuser", "testdb").Run()
	output, err := exec.Command("createdb", "-U", "testuser", "testdb").CombinedOutput()
	defer exec.Command("dropdb", "-U", "testuser", "testdb").Run()
	if err != nil {
		t.Fatalf("couldn't create postgres db: %v, %s\n", err, output)
	}

	//create the mysql database
	mysqlTestDB := func(command string, input ...string) ([]byte, error) {
		cmd := exec.Command("mysqladmin", "-u", "testuser", command, "testdb")
		//passing passwords as command-line args in mysqladmin is broken: A space makes it prompt.
		cmd.Env = []string{"MYSQL_PWD=testpassword"}
		cmd.Stdin = strings.NewReader(strings.Join(input, " "))
		return cmd.CombinedOutput()
	}
	_, _ = mysqlTestDB("drop", "yes")
	output, err = mysqlTestDB("create")
	if err != nil {
		t.Fatalf("couldn't create mysql db: %v, %s\n", err, string(output))
	}
	defer mysqlTestDB("drop", "yes")

	pg, err := sqlx.Connect("postgres", "postgres://testuser:testpassword@localhost/testdb?sslmode=disable")
	if err != nil {
		t.Fatalf("couldn't connect to postgres test db: %v\n", err)
	}

	mysql, err := sqlx.Connect("mysql", "testuser:testpassword@/testdb")
	if err != nil {
		t.Fatalf("couldn't connect to mysql test db: %v\n", err)
	}

	pg.SetMaxOpenConns(30)
	mysql.SetMaxOpenConns(30)

	t.Run("database lock", func(t *testing.T) {
		testDatabaseLock(t, pg)
		testDatabaseLock(t, mysql)
	})

	t.Run("values-preserved", func(t *testing.T) {
		testValuesPreserved(t, pg)
		testValuesPreserved(t, mysql)
	})

	t.Run("prereq", func(t *testing.T) {
		testPrereq(t, pg)
		testPrereq(t, mysql)
	})

	t.Run("whitespace", func(t *testing.T) {
		testWhitespace(t, pg)
		testWhitespace(t, mysql)
	})
}

func testValuesPreserved(t *testing.T, db *sqlx.DB) {
	registered = nil
	Register(
		`create table rerun(id int)`,
	)

	err := Run(db)
	if err != nil {
		t.Fatalf("running migrations: %v\n", err)
	}

	_, err = db.Exec(`insert into rerun(id) values (42)`)
	if err != nil {
		t.Fatalf("inserting placeholder value: %v\n", err)
	}

	Register(
		`create table rerun(id int)`,
		`alter table rerun add column name text`,
	)

	var val int
	err = db.Get(&val, `select id from rerun limit 1`)
	if err != nil {
		t.Fatalf("getting placeholder value: %v\n", err)
	}

	if val != 42 {
		t.Fatalf(`val != 42, val == "%v"`, val)
	}

}

func testPrereq(t *testing.T, db *sqlx.DB) {
	registered = nil
	Register(
		Prereq(` select 1 from test_prereq`),
		`alter table test_prereq add column food varchar(20)`,
	)

	Register(
		`create table test_prereq(dummy int)`,
	)

	err := Run(db)
	if err != nil {
		t.Fatalf("couldn't run migrations: %v\n", err)
	}

	_, err = db.Exec("insert into test_prereq(food) values ('nachos'), ('burritos')")
	if err != nil {
		t.Fatalf("couldn't run migration: %v\n", err)
	}

	var result []struct {
		Food string
	}
	err = db.Select(&result, "select food from test_prereq")
	if err != nil {
		t.Fatalf(": %v\n", err)
	}

	if len(result) != 2 {
		t.Fatalf(`len(result) != 2, len(result) == "%v"`, len(result))
	}
}

func testWhitespace(t *testing.T, db *sqlx.DB) {
	registered = nil
	Register(
		`
			--comments shouldn't affect things...
			create table test_whitespace(
				survive int
			)
		`,
	)

	err := Run(db)
	if err != nil {
		t.Fatalf(": %v\n", err)
	}

	//insert a value which is expected to survive the migration below
	_, err = db.Exec("insert into test_whitespace values (42)")
	if err != nil {
		t.Fatalf("couldn't insert: %v\n", err)
	}

	//this is the same migration, except for whitespace differences
	registered = nil
	Register(
		strings.Join([]string{
			"create table test_whitespace(",
			"survive int",
			")",
		}, "\n"),
	)

	if err = Run(db); err != nil {
		t.Fatalf(": %v\n", err)
	}

	// check if the value survived as expected
	var result struct {
		Survive int
	}

	if err = db.Get(&result, "select * from test_whitespace"); err != nil {
		t.Fatalf("couldn't select: %v\n", err)
	}

	if result.Survive != 42 {
		t.Fatalf(`result.Survive != 42, result.Survive == "%v"`, result.Survive)
	}
}

func testDatabaseLock(t *testing.T, db *sqlx.DB) {
	const count = 20
	counts := map[int]int{}
	wait := sync.WaitGroup{}
	wait.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			err := WithDatabaseLock(db, 30*time.Second, func() error {
				l := len(counts)
				counts[l]++
				return nil
			})
			if err != nil {
				t.Fatalf("error locking: %v\n", err)
			}
			wait.Done()
		}()
	}

	wait.Wait()

	if len(counts) != count {
		t.Fatalf(`len(counts) != %d, len(counts) == "%d"`, count, len(counts))
	}
}
