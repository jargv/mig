package migration

import (
	"crypto/md5"
	"encoding/base64"

	"github.com/jmoiron/sqlx"
)

var registeredMigrationSets []MigrationSet

type Step struct {
	Migrate string
	Revert  string
}

type MigrationSet []Step

func Register(set MigrationSet) {
	registeredMigrationSets = append(registeredMigrationSets, set)
}

func Run(db *sqlx.DB) error {
	for _, set := range registeredMigrationSets {
		err := runSet(db, set)
		if err != nil {
			return err
		}
	}

	return nil
}

func runSet(db *sqlx.DB, steps []Step) error {
	err := checkMigrationTable(db)
	if err != nil {
		return err
	}

	rows, err := db.Query(`
	SELECT hash, down
	FROM   migration
	`)
	if err != nil {
		return err
	}

	recordedMigrations := map[string]string{}
	for rows.Next() {
		var hash, down string
		err := rows.Scan(&hash, &down)
		if err != nil {
			break
		}
		recordedMigrations[hash] = down
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for len(steps) > 0 {
		hash := createHash(steps[0].Migrate)
		if _, ok := recordedMigrations[hash]; !ok {
			break //we've found the migration to rewind to!
		}
		delete(recordedMigrations, hash)
		steps = steps[1:]
	}

	// do the rewind process
	// TODO: do this in reverse chronology!
	for hash, down := range recordedMigrations {
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

	//execute the remaining steps
	for _, migration := range steps {
		tx, err := db.Begin()

		if err != nil {
			return err
		}

		_, err = tx.Exec(migration.Migrate)

		if err != nil {
			tx.Rollback()
			return err
		}

		hash := createHash(migration.Migrate)

		_, err = tx.Exec(`
		INSERT into migration (hash, down)
		VALUES ($1, $2);
		`, hash, migration.Revert)

		if err != nil {
			tx.Rollback()
			return err
		}

		err = tx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
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
