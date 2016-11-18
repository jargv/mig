package mig

import (
	"fmt"
	"time"
)

type series struct {
	steps []Step
}

func (s *series) done() bool {
	return len(s.steps) == 0
}

func (s *series) skipRecordedSteps(recorded map[string]recordedStep) {
	for len(s.steps) > 0 {
		hash := s.steps[0].hash
		if _, ok := recorded[hash]; !ok {
			break //we've found the migration to start at. Everything else must be redone.
		}
		delete(recorded, hash)
		s.steps = s.steps[1:]
	}
}

func (s *series) tryProgress(db DB) (bool, error) {
	progress := false

	for len(s.steps) > 0 {
		step := s.steps[0]

		if len(step.Prereq) > 0 {
			_, err := db.Exec(step.Prereq)
			if err != nil {
				return progress, nil
			}
		}

		s.steps = s.steps[1:]

		tx, err := db.Begin()

		if err != nil {
			_ = tx.Rollback()
			return false, err
		}

		_, err = tx.Exec(step.Migrate)

		if err != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf(
				"couldn't execute migration '%s': %v\n"+
					"file: %s\n"+
					"sql: `%s`",
				step.Name, err, step.file, step.Migrate,
			)
		}

		now := time.Now()
		stmt := fmt.Sprintf(`
		INSERT into MIG_RECORDED_MIGRATIONS (name, file, hash, pkg, revert, time)
		VALUES (%s, %s, %s, %s, %s, %s);
		`, arg(db, 1), arg(db, 2), arg(db, 3), arg(db, 4), arg(db, 5), arg(db, 6))
		_, err = tx.Exec(stmt, step.Name, step.file, step.hash, step.pkg, step.revert(), now)
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
