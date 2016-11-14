package mig

import "sort"

type recordedStep struct {
	Revert string
	Hash   string
	Order  int
}

func orderRecordedReverts(reverts map[string]recordedStep) []recordedStep {
	vals := []recordedStep{}
	for _, val := range reverts {
		vals = append(vals, val)
	}

	sort.Sort(byOrderDescending(vals))

	return vals
}

type byOrderDescending []recordedStep

func (r byOrderDescending) Less(i, j int) bool {
	return r[i].Order > r[j].Order
}

func (r byOrderDescending) Len() int {
	return len(r)
}

func (r byOrderDescending) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
}
