package mig

import (
	"errors"
	"log"
	"sync"
	"time"
)

var mutex = sync.Mutex{}
var ErrDatabaseLockTimout = errors.New("mig.WithDatabaseLock timed out")

func WithDatabaseLock(db DB, timeout time.Duration, callback func() error) error {
	start := time.Now()

	_, _ = db.Exec(`
		CREATE TABLE MIG_DATABASE_LOCK (
			lock_row int,
			UNIQUE (lock_row)
		)
	`)

	for {
		_, err := db.Exec(`
			INSERT INTO MIG_DATABASE_LOCK (lock_row)
			VALUES      (1)
		`)
		if err == nil {
			break
		}

		log.Printf("err: %#v", err)

		if time.Now().Sub(start) > timeout {
			return ErrDatabaseLockTimout
		}

		time.Sleep(1500 * time.Millisecond)
	}

	defer func() {
		_, _ = db.Exec(`
			DELETE FROM MIG_DATABASE_LOCK
		`)
	}()

	return callback()
}
