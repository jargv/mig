package mig

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

var mutex = sync.Mutex{}
var ErrDatabaseLockTimout = errors.New("mig.WithDatabaseLock timed out")

func WithDatabaseLock(db DB, timeout time.Duration, callback func() error) error {
	start := time.Now()

	if db.DriverName() == "mysql" {
		_, _ = db.Exec(`
			CREATE TABLE MIG_DATABASE_LOCK_V2 (
				id       BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
				lock_row INT,
				UNIQUE   (lock_row)
			)
		`)
	} else {
		log.Fatalf("mig.WithDatabaseLock not supported for driver: '%s'", db.DriverName())
	}

	var lockId int64
	for {
		res, err := db.Exec(`
			INSERT INTO MIG_DATABASE_LOCK_V2 (lock_row)
			VALUES      (1)
		`)
		if err == nil {
			lockId, err = res.LastInsertId()
			if err != nil {
				log.Fatalf("error trying to get LastInsertId: %s", err)
			} else {
				break
			}
		}

		log.Printf("(expected error) attempt to acquire db lock: %v\n", err)

		if time.Now().Sub(start) > timeout {
			return ErrDatabaseLockTimout
		}

		// variable backoff between 0.5 and 1.5 seconds
		sleepTime := time.Duration((0.5 + rand.Float32()) * float32(time.Second))
		time.Sleep(sleepTime)
	}

	defer func() {
		for {
			_, err := db.Exec(`
				DELETE FROM MIG_DATABASE_LOCK_V2
				WHERE id = ?
			`, lockId)

			if err == nil {
				break
			}

			log.Fatalf("error releasing lock: %v", err)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return callback()
}
