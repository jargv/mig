// +build mig_forward

package mig

const forward_only = true

type Step struct {
	Name    string
	Migrate string
	Prereq  string
	hash    string
	file    string
	pkg     string
	order   int
}

func (step *Step) revert() string {
	return ""
}

func (s *Step) setRevert(string) {
}
