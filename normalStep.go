// +build !mig_forward

package mig

import "strings"

const forward_only = false

type Step struct {
	Name    string
	Migrate string
	Prereq  string
	Revert  string
	hash    string
	file    string
	pkg     string
}

func (step *Step) revert() string {
	return step.Revert
}

func (s *Step) cleanWhitespace() {
	// we don't need whitespace in the db
	s.Revert = cleanWhitespace(s.Revert)
	// we want the hash to be invariant to whitespace
	s.Migrate = cleanWhitespace(s.Migrate)
}

func cleanWhitespace(str string) string {
	var resultLines []string
	lines := strings.Split(str, "\n")
	for _, line := range lines {
		line := strings.TrimSpace(line)

		//skip empty lines
		if len(line) == 0 {
			continue
		}

		//skip comments
		if len(line) >= 2 && line[0:2] == "--" {
			continue
		}

		resultLines = append(resultLines, line)
	}

	return strings.Join(resultLines, "\n")
}
