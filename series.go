package mig

import (
	"fmt"
	"log"
	"strings"
	"time"
)

type series struct {
	steps       []step
	currentStep int
}

func (s *series) done() bool {
	return s.currentStep >= len(s.steps)
}

func (s *series) currentStepIsAlreadyDone(hashes map[string]struct{}) bool {
	currentHash := s.steps[s.currentStep].hash
	_, ok := hashes[currentHash]
	return ok
}

func (s *series) tryProgress(db DB, hashes map[string]struct{}) (bool, error) {
	progress := false

	for ; s.currentStep < len(s.steps); s.currentStep++ {
		if s.currentStepIsAlreadyDone(hashes) {
			continue
		}

		step := s.steps[s.currentStep]

		if step.isPrereq {
			_, err := db.Exec(step.migrate)
			if err != nil {
				return progress, nil
			}
			continue //prereqs don't have migrations after them
		}

		tx, err := db.Begin()
		defer tx.Rollback()

		if err != nil {
			return false, err
		}

		log.Printf("executing migration: %.60s\n", strings.Replace(step.migrate, "\n", "\\n", -1))
		if step.migrateFunc != nil {
			err = step.migrateFunc(tx)
		} else {
			_, err = tx.Exec(step.migrate)
		}

		if err != nil {
			return false, fmt.Errorf(
				"couldn't execute migration': %v\n"+
					"file: %s\n"+
					"sql: `%s`",
				"hash: `%s`",
				err, step.file, step.migrate, step.hash,
			)
		}
		log.Printf("finished migration: %.60s\n", strings.Replace(step.migrate, "\n", "\\n", -1))

		now := time.Now()
		stmt := fmt.Sprintf(`
			INSERT into MIG_RECORDED_MIGRATIONS (sql_text, file, hash, pkg, time)
			VALUES (%s, %s, %s, %s, %s);
		`, arg(db, 1), arg(db, 2), arg(db, 3), arg(db, 4), arg(db, 5))
		_, err = tx.Exec(stmt, step.migrate, step.file, step.hash, step.pkg, now)
		if err != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf(
				"internal mig error (couldn't insert into MIG_RECORDED_MIGRATIONS table): %v", err,
			)
		}

		err = tx.Commit()
		if err != nil {
			return false, fmt.Errorf("couldn't commit transaction: %v", err)
		}

		progress = true
	}

	return progress, nil
}
