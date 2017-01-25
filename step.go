package mig

type Step struct {
	Name    string
	Migrate string
	Prereq  string

	hash  string
	file  string
	pkg   string
	order int
}
