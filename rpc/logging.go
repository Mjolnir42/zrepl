package rpc

type Logger interface {
	Printf(format string, args ...interface{})
}

type noLogger struct{}

func (l noLogger) Printf(format string, args ...interface{}) {}
