package mig

import (
	"fmt"
	"time"
)

type series struct {
	steps       []step
	currentStep int
}

func (s *series) done() bool {
	return s.currentStep >= len(s.steps)
}

func (s *series) syncWithRecordedSteps(hashes map[string]struct{}) {
	var i int
	for i = 0; i < len(s.steps); i++ {
		hash := s.steps[i].hash

		_, ok := hashes[hash]

		if !ok {
			break
		}

		delete(hashes, hash)
	}
	s.currentStep = i
}

func (s *series) tryProgress(db DB) (bool, error) {
	progress := false

	for ; s.currentStep < len(s.steps); s.currentStep++ {
		step := s.steps[s.currentStep]

		if step.isPrereq {
			_, err := db.Exec(step.migrate)
			if err != nil {
				return progress, nil
			}
			continue //prereqs don't have migrations after them
		}

		tx, err := db.Begin()

		if err != nil {
			_ = tx.Rollback()
			return false, err
		}

		_, err = tx.Exec(step.migrate)

		if err != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf(
				"couldn't execute migration': %v\n"+
					"file: %s\n"+
					"sql: `%s`",
				err, step.file, step.migrate,
			)
		}

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
			tx.Rollback()
			return false, fmt.Errorf("couldn't commit transaction: %v", err)
		}

		progress = true
	}

	return progress, nil

}
