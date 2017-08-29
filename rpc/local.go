package rpc

import "reflect"

type LocalRPC struct {
	endpoints map[string]reflect.Value
}

func NewLocalRPC() *LocalRPC {
	panic("not implemented")
	return nil
}

func (s *LocalRPC) RegisterEndpoint(name string, handler interface{}) (err error) {
	panic("not implemented")
	return nil
}

func (s *LocalRPC) Serve() (err error) {
	panic("not implemented")
	return nil
}

func (c *LocalRPC) Call(endpoint string, in, out interface{}) (err error) {
	panic("not implemented")
	return nil
}

func (c *LocalRPC) Close() (err error) {
	panic("not implemented")
	return nil
}
