package rpc

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

type Frame struct {
	Type    FrameType
	Flags   uint8
	Length  uint64
	Payload []byte
}

//go:generate stringer -type=FrameType
type FrameType uint8

const (
	FrameTypeHeader  FrameType = 0x01
	FrameTypeData    FrameType = 0x02
	FrameTypeTrailer FrameType = 0x03
	FrameTypeRST     FrameType = 0xff
)

type DataFrameFlags uint8

const (
	DataFrameFlagMore = 0x01
)

type FrameLayer struct {
	rwc io.ReadWriteCloser
}

func NewFrameLayer(rwc io.ReadWriteCloser) *FrameLayer {
	return &FrameLayer{rwc}
}

const FRAME_HEADER_LENGTH = 1 + 1 + 8
const FRAME_PAYLOAD_LENGTH = 4 * 1024 * 1024
const MAX_FRAME_LENGTH = FRAME_HEADER_LENGTH + FRAME_PAYLOAD_LENGTH

func (l *FrameLayer) ReadFrame() (f *Frame, err error) {

	f = &Frame{}

	err = binary.Read(l.rwc, binary.LittleEndian, &f.Type)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = binary.Read(l.rwc, binary.LittleEndian, &f.Flags)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = binary.Read(l.rwc, binary.LittleEndian, &f.Length)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if f.Length > MAX_FRAME_LENGTH {
		return nil, errors.Errorf("frame length exceeds max frame length set in server")
	}

	f.Payload = make([]byte, f.Length)
	_, err = io.ReadFull(l.rwc, f.Payload)

	return f, nil
}

func (l *FrameLayer) WriteFrame(f *Frame) (err error) {

	if len(f.Payload) > FRAME_PAYLOAD_LENGTH {
		return errors.Errorf("frame length exceeds max frame length set in server")
	}

	// Compute frame length
	f.Length = FRAME_HEADER_LENGTH + len(f.Payload)

	err = binary.Write(l.rwc, binary.LittleEndian, &f.Type)
	if err != nil {
		return errors.WithStack(err)
	}
	err = binary.Write(l.rwc, binary.LittleEndian, &f.Flags)
	if err != nil {
		return errors.WithStack(err)
	}
	err = binary.Write(l.rwc, binary.LittleEndian, &f.Length)
	if err != nil {
		return errors.WithStack(err)
	}

	for i := 0; uint64(i) < f.Length; {
		n, err := l.rwc.Write(f.Payload[i:])
		if err != nil {
			return errors.WithStack(err)
		}
		i += n
	}
	return err
}

func (l *FrameLayer) Close() (err error) {
	return l.rwc.Close()
}

type MessageLayer struct {
	fl *FrameLayer
}

func NewMessageLayer(fl *FrameLayer) *MessageLayer {
	return &MessageLayer{fl}
}

func (l *MessageLayer) HangUp() (err error) {
	rstFrameError := l.fl.WriteFrame(&Frame{Type: FrameTypeRST})
	closeErr := l.fl.Close()
	if rstFrameError != nil {
		return rstFrameError
	} else {
		return closeErr
	}
}

func (l *MessageLayer) ReadHeader() (h *Header, err error) {

	f, err := l.fl.ReadFrame()
	if err != nil {
		return nil, err
	}

	if f.Type != FrameTypeHeader {
		err = errors.Errorf("expecting header frame")
		return nil, err
	}

	h = &Header{}
	err = json.Unmarshal(f.Payload, &h)
	return h, err

}

func (l *MessageLayer) WriteHeader(h *Header) (err error) {
	f = &Frame{}
	f.Type = FrameTypeHeader
	f.Payload, err = json.Marshal(h)
	if err != nil {
		return errors.Wrap(err, "cannot encode header, probably fatal")
	}
	return l.fl.WriteFrame(f)
}

type dataReader struct {
	fl        *FrameLayer
	lastFrame *Frame
}

func (r *dataReader) Read(p []byte) (n int, err error) {
	if r.lastFrame == nil {
		f, err := r.fl.ReadFrame()
		if err != nil {
			return 0, errors.Wrap(err, "cannot read frame")
		}
		if f.Type != FrameTypeData {
			return 0, errors.Wrap(err, "expected data frame")
		}
		r.lastFrame = f
	}
	n, err = r.lastFrame.Payload.Read(p)
	if err == io.EOF {
		// we reached the end of this frame
		if r.lastFrame.Flags&DataFrameFlagMore != 0 {
			// there are more data frames to come
			r.lastFrame = nil
			err = nil
		}
	}
	return
}

func (l *MessageLayer) ReadData() (dr *dataReader) {
	dr = &dataReader{l.fl, nil}
	return dr
}

type dataWriter struct {
	fl *FrameLayer
	f  *Frame
}

func (d *dataWriter) Write(p []byte) (n int, err error) {
	if d.f == nil {
		d.f = d.fl.GetFrame()
	}
	n, err := d.f.Payload.Write(p)
}

func (d *dataWriter) Close() error {
	panic("not implemented")
}

func (l *MessageLayer) WriteData() (dw *dataWriter) {
	return nil
}

type Status uint64

const (
	StatusOK Status = 1 + iota
	StatusRequestError
	StatusServerError
)

type Header struct {
	// Request-only
	Endpoint string
	// Data type of body (request & reply)
	DataType DataType
	// Request-only
	Accept DataType
	// Reply-only
	Error Status
	// Reply-only
	ErrorMessage string
}

func NewErrorHeader(status Status, format string, args ...interface{}) (h *Header) {
	h = &Header{}
	h.Error = status
	h.ErrorMessage = fmt.Sprintf(format, args...)
	return
}

type DataType uint8

const (
	DataTypeMarshaledJSON DataType = 1 + iota
	DataTypeOctets
)
