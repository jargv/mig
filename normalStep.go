// +build !mig_forward

package mig

const forward_only = false

type Step struct {
	Name    string
	Migrate string
	Prereq  string
	Revert  string
	hash    string
	file    string
	pkg     string
	order   int
}

func (step *Step) revert() string {
	return step.Revert
}

func (s *Step) setRevert(revert string) {
	s.Revert = revert
}
