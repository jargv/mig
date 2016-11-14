package mig

import (
	"os/exec"
	"strings"
	"testing"

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
	mysqlTestDB("drop", "yes")
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

	t.Run("revert", func(t *testing.T) {
		testRevert(t, pg)
		testRevert(t, mysql)
	})

	t.Run("prereq", func(t *testing.T) {
		testPrereq(t, pg)
		testPrereq(t, mysql)
	})

	t.Run("whitespace", func(t *testing.T) {
		testWhitespace(t, pg)
		testWhitespace(t, mysql)
	})

	t.Run("revert order", func(t *testing.T) {
		testRevertOrder(t, pg)
		testRevertOrder(t, mysql)
	})

	_ = mysql

}

type reg struct{}

func (r *reg) Register() {
	registeredMigrations = nil
	Register([]Step{
		{
			Migrate: `create table survive(val int)`,
		},
		{
			Migrate: `
			create table test_user (
				name TEXT,
				food TEXT
			)
			`,
			Revert: `drop table test_user`,
		},
	})
}

func testRevert(t *testing.T, db *sqlx.DB) {
	var reg reg
	//register from a method, just to test that case
	reg.Register()

	err := Run(db)
	if err != nil {
		t.Fatalf(": %v\n", err)
	}

	stmt := `insert into survive (val) values (42)`
	if _, err = db.Exec(stmt); err != nil {
		t.Fatalf("couldn't insert: %v\n", err)
	}

	stmt = `insert into test_user (name, food) values ('Jonathan', 'crab'), ('Sarah', 'ice cream')`
	if _, err = db.Exec(stmt); err != nil {
		t.Fatalf("couldn't insert: %v\n", err)
	}

	var result1 []struct {
		Name string `db:"name"`
		Food string `db:"food"`
	}

	db.Select(&result1, "select * from test_user")
	if len(result1) != 2 {
		t.Fatalf(`len(result) != 2, len(result) == "%v"`, len(result1))
	}

	//reset the migration set
	registeredMigrations = nil
	Register([]Step{
		{
			Migrate: `create table survive(val int)`,
		},
		{
			Migrate: `
				create table test_user (
					name TEXT,
					tv   TEXT
				)
			`,
		},
	})
	err = Run(db)
	if err != nil {
		t.Fatalf(": %v\n", err)
	}

	stmt = `insert into test_user (name, tv) values ('Jonathan', 'Rick and Morty'), ('Sarah', 'The Office')`
	if _, err = db.Exec(stmt); err != nil {
		t.Fatalf("couldn't insert: %v\n", err)
	}

	var result2 []struct {
		Name string `db:"name"`
		Tv   string `db:"tv"`
	}
	db.Select(&result2, "select * from test_user")
	if len(result2) != 2 {
		t.Fatalf(`len(result2) != 2, len(result2) == "%v"`, len(result2))
	}

	var surviver struct {
		Val int
	}
	err = db.Get(&surviver, `select val from survive limit 1`)
	if err != nil {
		t.Fatalf("table 'survive' didn't survive as expected: %v\n", err)
	}
	if surviver.Val != 42 {
		t.Fatalf(`surviver.Val != 42, surviver.Val == "%v"`, surviver.Val)
	}
}

func testPrereq(t *testing.T, db *sqlx.DB) {

	registeredMigrations = nil
	Register([]Step{
		{
			Prereq: `
			  select 1 from test_prereq
			`,
			Migrate: `
			  alter table test_prereq add column food varchar(20)
			`,
		},
	})

	Register([]Step{
		{
			Migrate: `
			  create table test_prereq(dummy int)
			`,
		},
	})

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
	registeredMigrations = nil
	Register([]Step{
		{
			Revert: `drop table test_whitespace`,
			Migrate: `
			  --comments shouldn't affect things...
				create table test_whitespace(
					survive int
				)
			`,
		},
	})

	err := Run(db)
	if err != nil {
		t.Fatalf(": %v\n", err)
	}

	//insert a value which is expected to survive the migration below
	_, err = db.Exec("insert into test_whitespace values (42)")
	if err != nil {
		t.Fatalf("couldn't insert: %v\n", err)
	}

	registeredMigrations = nil

	//this is the same migration, except for whitespace differences
	registeredMigrations = nil
	Register([]Step{
		{
			Revert: `drop table test_whitespace`,
			Migrate: strings.Join([]string{
				"create table test_whitespace(",
				"survive int",
				")",
			}, "\n"),
		},
	})

	err = Run(db)
	if err != nil {
		t.Fatalf(": %v\n", err)
	}

	//check if the value survived as expected
	var result struct {
		Survive int
	}
	err = db.Get(&result, "select * from test_whitespace")
	if err != nil {
		t.Fatalf("couldn't select: %v\n", err)
	}

	if result.Survive != 42 {
		t.Fatalf(`result.Survive != 42, result.Survive == "%v"`, result.Survive)
	}
}

func testRevertOrder(t *testing.T, db *sqlx.DB) {
	registeredMigrations = nil
	Register([]Step{
		{
			Revert: `drop table mig_users`,
			Migrate: `
				create table mig_users(
					id bigint,

					UNIQUE (id)
				)
			`,
		},
		{
			Revert: `drop table mig_orders`,
			Migrate: `
			create table mig_orders(
				id       BIGINT,
				customer BIGINT,

				FOREIGN KEY (customer) references mig_users (id)
			)
			`,
		},
	})

	err := Run(db)
	if err != nil {
		t.Fatalf("running migration: %v\n", err)
	}

	registeredMigrations = nil
	Register([]Step{
		{
			Revert: `drop table mig_users`,
			Migrate: `
			create table mig_users(
				id        bigint,
				newcolumn int
			)
			`,
		},
		{
			Revert: `drop table mig_orders`,
			Migrate: `
			create table mig_orders(
				id bigint
			)
			`,
		},
	})

	err = Run(db)
	if err != nil {
		t.Fatalf(": %v\n", err)
	}
}
