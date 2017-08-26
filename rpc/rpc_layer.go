package rpc

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"

	"github.com/pkg/errors"
)

type Server struct {
	ml        *MessageLayer
	logger    Logger
	endpoints map[string]endpointDescr
}

type typeMap struct {
	local reflect.Type
	proto DataType
}
type endpointDescr struct {
	inType  typeMap
	outType typeMap
	handler reflect.Value
}

func typeIsIOReader(t reflect.Type) bool {
	return t == reflect.TypeOf((*io.Reader)(nil)).Elem()
}
func typeIsIOReaderPtr(t reflect.Type) bool {
	return t == reflect.TypeOf((*io.Reader)(nil))
}

func makeEndpointDescr(handler interface{}) (descr endpointDescr, err error) {

	ht := reflect.TypeOf(handler)

	if ht.Kind() != reflect.Func {
		err = errors.Errorf("handler must be of kind reflect.Func")
		return
	}

	if ht.NumIn() != 2 || ht.NumOut() != 1 {
		err = errors.Errorf("handler must have exactly two input parameters and one output parameter")
		return
	}
	if !(ht.In(0).Kind() == reflect.Ptr || typeIsIOReader(ht.In(0))) {
		err = errors.Errorf("input parameter must be a pointer or an io.Reader, is of kind %s, type %s", ht.In(0).Kind(), ht.In(0))
		return
	}
	if !(ht.In(1).Kind() == reflect.Ptr) {
		err = errors.Errorf("second input parameter (the non-error output parameter) must be a pointer or an *io.Reader")
		return
	}
	errInterfaceType := reflect.TypeOf((*error)(nil)).Elem()
	if !ht.Out(0).Implements(errInterfaceType) {
		err = errors.Errorf("handler must return an error")
		return
	}

	descr.handler = reflect.ValueOf(handler)
	descr.inType.local = ht.In(0)
	descr.outType.local = ht.In(1)

	if typeIsIOReader(ht.In(0)) {
		descr.inType.proto = DataTypeOctets
	} else {
		descr.inType.proto = DataTypeMarshaledJSON
	}

	if typeIsIOReaderPtr(ht.In(1)) {
		descr.outType.proto = DataTypeOctets
	} else {
		descr.outType.proto = DataTypeMarshaledJSON
	}

	return
}

type MarshaledJSONEndpoint func(bodyJSON interface{})

func NewServer(rwc io.ReadWriteCloser) *Server {
	ml := NewMessageLayer(rwc)
	return &Server{
		ml, noLogger{}, make(map[string]endpointDescr),
	}
}

func (s *Server) SetLogger(logger Logger) {
	s.logger = logger
	s.ml.logger = logger
}

func (s *Server) RegisterEndpoint(name string, handler interface{}) (err error) {
	_, ok := s.endpoints[name]
	if ok {
		return errors.Errorf("already set up an endpoint for '%s'", name)
	}
	s.endpoints[name], err = makeEndpointDescr(handler)
	return
}

func (s *Server) ServeConn() (err error) {

	ml := s.ml
	h, err := ml.ReadHeader()

	ep, ok := s.endpoints[h.Endpoint]
	if !ok {
		r := NewErrorHeader(StatusRequestError, "unregistered endpoint %s", h.Endpoint)
		err = ml.WriteHeader(r)
		if err != nil {
			return errors.WithStack(err)
		}
		return ml.HangUp()
	}

	if ep.inType.proto != h.DataType {
		r := NewErrorHeader(StatusRequestError, "wrong DataType for endpoint %s (has %s, you provided %s)", h.Endpoint, ep.inType.proto, h.DataType)
		err = ml.WriteHeader(r)
		if err != nil {
			return errors.WithStack(err)
		}
		return ml.HangUp()
	}
	if ep.outType.proto != h.Accept {
		r := NewErrorHeader(StatusRequestError, "wrong Accept for endpoint %s (has %s, you provided %s)", h.Endpoint, ep.outType.proto, h.Accept)
		err = ml.WriteHeader(r)
		if err != nil {
			return errors.WithStack(err)
		}
		return ml.HangUp()
	}

	dr := ml.ReadData()

	// Determine inval
	var inval reflect.Value
	switch ep.inType.proto {
	case DataTypeMarshaledJSON:
		// Unmarshal input
		inval = reflect.New(ep.inType.local.Elem())
		invalIface := inval.Interface()
		err = json.NewDecoder(dr).Decode(invalIface)
		if err != nil {
			r := NewErrorHeader(StatusRequestError, "cannot decode marshaled JSON: %s")
			err = ml.WriteHeader(r)
			if err != nil {
				return errors.WithStack(err)
			}
			return ml.HangUp()
		}
	case DataTypeOctets:
		// Take data as is
		inval = reflect.ValueOf(dr)
	default:
		panic("not implemented")
	}

	outval := reflect.New(ep.outType.local.Elem()) // outval is a double pointer

	s.logger.Printf("before handler, inval=%v outval=%v", inval, outval)

	// Call the handler
	errs := ep.handler.Call([]reflect.Value{inval, outval})

	if !errs[0].IsNil() {
		// send error header now and exit
		panic("not implemented")
		return ml.HangUp()
	}

	switch ep.outType.proto {

	case DataTypeMarshaledJSON:

		var dataBuf bytes.Buffer
		// Marshal output
		err = json.NewEncoder(&dataBuf).Encode(outval.Interface())
		if err != nil {
			r := NewErrorHeader(StatusServerError, "cannot marshal response: %s", err)
			err = ml.WriteHeader(r)
			if err != nil {
				return errors.WithStack(err)
			}
			return ml.HangUp()
		}

		replyHeader := Header{
			Error:    StatusOK,
			DataType: ep.outType.proto,
		}
		err = ml.WriteHeader(&replyHeader)
		if err != nil {
			return errors.WithStack(err)
		}

		err = ml.WriteData(&dataBuf)
		if err != nil {
			return errors.WithStack(err)
		}

	case DataTypeOctets:

		h := Header{
			DataType: DataTypeOctets,
		}
		err = ml.WriteHeader(&h)
		if err != nil {
			return errors.WithStack(err)
		}
		reader := outval.Interface().(*io.Reader) // we checked that when adding the endpoint
		err = ml.WriteData(*reader)
		if err != nil {
			// TODO send trailer? how should client know?
			return errors.WithStack(err)
		}

	}

	// Octets would have already been sent

	return nil

}
