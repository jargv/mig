package mig

import (
	"log"
	"os/exec"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func getConnection(t *testing.T) *sqlx.DB {
	exec.Command("dropdb", "testPostgres").Run()
	output, err := exec.Command("createdb", "testPostgres").CombinedOutput()
	if err != nil {
		log.Printf("couldn't create postgres db: %v, %s\n", err, output)
	}

	db, err := sqlx.Connect("postgres", "postgres://testuser:testpassword@localhost/testPostgres")
	if err != nil {
		t.Fatalf("couldn't connect to postgres test db: %v\n", err)
	}

	return db
}

func TestRevert(t *testing.T) {
	db := getConnection(t)
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

	clearStepsForNextTest()

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

func TestPrereq(t *testing.T) {
	db := getConnection(t)

	Register([]Step{
		{
			Prereq: `
			  select 1 from test_prereq
			`,
			Migrate: `
			  alter table test_prereq add column food text
			`,
		},
	})

	Register([]Step{
		{
			Migrate: `
			  create table test_prereq()
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
	err = db.Select(&result, "select * from test_prereq")
	if err != nil {
		t.Fatalf(": %v\n", err)
	}

	if len(result) != 2 {
		t.Fatalf(`len(result) != 2, len(result) == "%v"`, len(result))
	}
}
