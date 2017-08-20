package rpc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// debug log
var log devNullLog

type devNullLog struct{}

func (l devNullLog) Printf(format string, args ...interface{}) {}

type Frame struct {
	Type          FrameType
	NoMoreFrames  bool
	PayloadLength uint32
}

//go:generate stringer -type=FrameType
type FrameType uint8

const (
	FrameTypeHeader  FrameType = 0x01
	FrameTypeData    FrameType = 0x02
	FrameTypeTrailer FrameType = 0x03
	FrameTypeRST     FrameType = 0xff
)

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

const (
	MAX_PAYLOAD_LENGTH = 4 * 1024 * 1024
	MAX_HEADER_LENGTH  = 4 * 1024
)

type frameBridgingReader struct {
	l         *MessageLayer
	frameType FrameType
	// < 0 means no limit
	bytesLeftToLimit int
	f                Frame
}

func NewFrameBridgingReader(l *MessageLayer, frameType FrameType, totalLimit int) *frameBridgingReader {
	return &frameBridgingReader{l, frameType, totalLimit, Frame{}}
}

func (r *frameBridgingReader) Read(b []byte) (n int, err error) {
	if r.bytesLeftToLimit == 0 {
		return 0, io.EOF
	}
	if r.f.PayloadLength == 0 {

		if r.f.NoMoreFrames {
			err = io.EOF
			return
		}

		log.Printf("reading frame")
		r.f, err = r.l.readFrame()
		if err != nil {
			err = errors.Wrap(err, "error reading frame")
			return 0, err
		}
		log.Printf("read frame: %#v", r.f)
		if r.f.Type != r.frameType {
			err = errors.Wrapf(err, "expected frame of type %s", r.frameType)
			return 0, err
		}
	}
	maxread := len(b)
	if maxread > int(r.f.PayloadLength) {
		maxread = int(r.f.PayloadLength)
	}
	if r.bytesLeftToLimit > 0 && maxread > r.bytesLeftToLimit {
		maxread = r.bytesLeftToLimit
	}
	nb, err := r.l.rwc.Read(b[:maxread])
	log.Printf("read  %v from rwc\n", nb)
	if nb < 0 {
		panic("should not return negative number of bytes")
	}
	r.f.PayloadLength -= uint32(nb)
	r.bytesLeftToLimit -= nb
	return nb, err // TODO io.EOF for maxread = r.f.PayloadLength ?
}

type frameBridgingWriter struct {
	l         *MessageLayer
	frameType FrameType
	// < 0 means no limit
	bytesLeftToLimit int
	payloadLength    int
	buffer           *bytes.Buffer
}

func NewFrameBridgingWriter(l *MessageLayer, frameType FrameType, totalLimit int) *frameBridgingWriter {
	return &frameBridgingWriter{l, frameType, totalLimit, MAX_PAYLOAD_LENGTH, bytes.NewBuffer(make([]byte, 0, MAX_PAYLOAD_LENGTH))}
}

func (w *frameBridgingWriter) Write(b []byte) (n int, err error) {
	for n = 0; n < len(b); {
		i, err := w.writeUntilFrameFull(b[n:])
		n += i
		if err != nil {
			return n, errors.WithStack(err)
		}
	}
	return
}

func (w *frameBridgingWriter) writeUntilFrameFull(b []byte) (n int, err error) {
	if len(b) <= 0 {
		return
	}
	if w.bytesLeftToLimit == 0 {
		err = errors.Errorf("exceeded limit of total %v bytes for this message")
		return
	}
	maxwrite := len(b)
	remainingInFrame := w.payloadLength - w.buffer.Len()

	if maxwrite > remainingInFrame {
		maxwrite = remainingInFrame
	}
	if w.bytesLeftToLimit > 0 && maxwrite > w.bytesLeftToLimit {
		maxwrite = w.bytesLeftToLimit
	}
	w.buffer.Write(b[:maxwrite])
	w.bytesLeftToLimit -= maxwrite
	n = maxwrite
	if w.bytesLeftToLimit == 0 {
		err = w.flush(true)
	} else if w.buffer.Len() == w.payloadLength {
		err = w.flush(false)
	}
	return
}

func (w *frameBridgingWriter) flush(nomore bool) (err error) {

	f := Frame{w.frameType, nomore, uint32(w.buffer.Len())}
	err = w.l.writeFrame(f)
	if err != nil {
		errors.WithStack(err)
	}
	_, err = w.buffer.WriteTo(w.l.rwc)
	return
}

func (w *frameBridgingWriter) Close() (err error) {
	return w.flush(true)
}

type MessageLayer struct {
	rwc io.ReadWriteCloser
}

func NewMessageLayer(rwc io.ReadWriteCloser) *MessageLayer {
	return &MessageLayer{rwc}
}

func (l *MessageLayer) HangUp() (err error) {
	f := Frame{Type: FrameTypeRST}
	rstFrameError := l.writeFrame(f)
	closeErr := l.rwc.Close()
	if rstFrameError != nil {
		return rstFrameError
	} else {
		return closeErr
	}
}

func (l *MessageLayer) readFrame() (f Frame, err error) {
	err = binary.Read(l.rwc, binary.LittleEndian, &f.Type)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	err = binary.Read(l.rwc, binary.LittleEndian, &f.NoMoreFrames)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	err = binary.Read(l.rwc, binary.LittleEndian, &f.PayloadLength)
	if err != nil {
		err = errors.WithStack(err)
		return
	}
	if f.PayloadLength > MAX_PAYLOAD_LENGTH {
		err = errors.Errorf("frame exceeds max payload length")
		return
	}
	return
}

func (l *MessageLayer) writeFrame(f Frame) (err error) {
	err = binary.Write(l.rwc, binary.LittleEndian, &f.Type)
	if err != nil {
		return errors.WithStack(err)
	}
	err = binary.Write(l.rwc, binary.LittleEndian, &f.NoMoreFrames)
	if err != nil {
		return errors.WithStack(err)
	}
	err = binary.Write(l.rwc, binary.LittleEndian, &f.PayloadLength)
	if err != nil {
		return errors.WithStack(err)
	}
	if f.PayloadLength > MAX_PAYLOAD_LENGTH {
		err = errors.Errorf("frame exceeds max payload length")
		return
	}
	return
}

func (l *MessageLayer) ReadHeader() (h *Header, err error) {
	r := NewFrameBridgingReader(l, FrameTypeHeader, MAX_HEADER_LENGTH)
	h = &Header{}
	err = json.NewDecoder(r).Decode(&h)
	return h, err
}

func (l *MessageLayer) WriteHeader(h *Header) (err error) {
	w := NewFrameBridgingWriter(l, FrameTypeHeader, MAX_HEADER_LENGTH)
	err = json.NewEncoder(w).Encode(h)
	if err != nil {
		return errors.Wrap(err, "cannot encode header, probably fatal")
	}
	w.Close()
	return
}

func (l *MessageLayer) ReadData() (reader io.Reader) {
	r := NewFrameBridgingReader(l, FrameTypeData, -1)
	return r
}

func (l *MessageLayer) WriteData(source io.Reader) (err error) {
	w := NewFrameBridgingWriter(l, FrameTypeData, -1)
	_, err = io.Copy(w, source)
	if err != nil {
		return errors.WithStack(err)
	}
	err = w.Close()
	return
}