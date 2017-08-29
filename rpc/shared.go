package rpc

import (
	"fmt"
	"io"
	"reflect"
)

type RPCServer interface {
	Serve() (err error)
	RegisterEndpoint(name string, handler interface{}) (err error)
}

type RPCClient interface {
	Call(endpoint string, in, out interface{}) (err error)
	Close() (err error)
}

type Logger interface {
	Printf(format string, args ...interface{})
}

type noLogger struct{}

func (l noLogger) Printf(format string, args ...interface{}) {}
func typeIsIOReader(t reflect.Type) bool {
	return t == reflect.TypeOf((*io.Reader)(nil)).Elem()
}

func typeIsIOReaderPtr(t reflect.Type) bool {
	return t == reflect.TypeOf((*io.Reader)(nil))
}

// An error returned by the Client if the response indicated a status code other than StatusOK
type RPCError struct {
	ResponseHeader *Header
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("%s: %s", e.ResponseHeader.Error, e.ResponseHeader.ErrorMessage)
}

type RPCProtoError struct {
	Message         string
	UnderlyingError error
}

func (e *RPCProtoError) Error() string {
	return e.Message
}
