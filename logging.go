package mig

import stdlog "log"

// Logger is an interface for logging. It is a subset of
// log.Logger. If you don't want errors to be fatal, 
// you can implement this interface for a type which doesn't
// call os.Exit() as standard log.Fatal() does.
type Logger interface {
	Printf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

type noopLogger struct {
}

func (nl *noopLogger) Printf(format string, args ...interface{}) {
}

func (nl *noopLogger) Fatalf(format string, args ...interface{}) {
	stdlog.Fatalf(format, args...)
}

var log Logger = &noopLogger{}

func SetLogger(l Logger) {
	log = l
}
