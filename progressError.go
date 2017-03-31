package mig

import (
	"bytes"
	"fmt"
)

type progressError struct {
	series []*series
}

func (pe *progressError) Error() string {
	buffer := &bytes.Buffer{}
	fmt.Fprintf(buffer, "Unable to make progress on migrations")
	for _, series := range pe.series {
		if series.done() {
			continue
		}
		prereq := series.steps[series.currentStep]
		fmt.Fprintf(buffer, "\n\t%s (%s)", prereq.migrate, prereq.file)
	}
	return buffer.String()
}
