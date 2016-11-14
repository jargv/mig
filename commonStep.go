package mig

import (
	"crypto/md5"
	"encoding/base64"
)

func (s *Step) computeHash() {
	sum := md5.Sum([]byte(s.Migrate))
	b64 := base64.StdEncoding.EncodeToString(sum[:])
	s.hash = string(b64[:])
}
