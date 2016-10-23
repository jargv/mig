package mig

import (
	"crypto/md5"
	"encoding/base64"
	"strings"
)

// Step represents a single step in a migration
type Step struct {
	Migrate string
	Revert  string
	Prereq  string
	hash    string
}

func (s *Step) cleanWhitespace() {
	// we don't need whitespace in the db
	s.Revert = cleanWhitespace(s.Revert)

	// we want the hash to be invariant to whitespace
	s.Migrate = cleanWhitespace(s.Migrate)
}

func (s *Step) computeHash() {
	sum := md5.Sum([]byte(s.Migrate))
	b64 := base64.StdEncoding.EncodeToString(sum[:])
	s.hash = string(b64[:])
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
