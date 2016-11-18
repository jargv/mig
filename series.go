package mig

import (
	"fmt"
	"time"
)

type series struct {
	steps       []Step
	currentStep int
}

func (s *series) done() bool {
	return s.currentStep >= len(s.steps)
}

func (s *series) syncWithRecordedSteps(recorded map[string]recordedStep) {
	var i int
	for i = 0; i < len(s.steps); i++ {
		hash := s.steps[i].hash

		r, ok := recorded[hash]

		if !ok {
			break
		}

		delete(recorded, hash)
		s.steps[i].order = r.Order
	}
	s.currentStep = i
}

func (s *series) rewindToRevertPoint(revertPoint int) {
	for s.currentStep > 1 && s.steps[s.currentStep-1].order >= revertPoint {
		s.currentStep--
	}
}

func (s *series) tryProgress(db DB) (bool, error) {
	progress := false

	for ; s.currentStep < len(s.steps); s.currentStep++ {
		step := s.steps[s.currentStep]

		if len(step.Prereq) > 0 {
			_, err := db.Exec(step.Prereq)
			if err != nil {
				return progress, nil
			}
		}

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
