package rowmessage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"

	"github.com/gogo/protobuf/proto"

	"tabler/heka"
)

type RowMessage interface {
	GetType() string
	GetValue(string) interface{}
}

type MapRowMessage interface {
	RowMessage
	GetMap() map[string]interface{}
}

type SliceRowMessage interface {
	RowMessage
	GetNumColumns() int
	GetColumn(int) (string, interface{})
}

type MessageReader interface {
	ReadMsg() (RowMessage, error)
}

//
// JSONRowMessage
//

type JSONRowMessage map[string]interface{}

func (m JSONRowMessage) GetType() string {
	return m["type"].(string)
}

func (m JSONRowMessage) GetValue(name string) interface{} {
	return m[name]
}

func (m JSONRowMessage) GetMap() JSONRowMessage {
	return m
}

var EndOfInput = fmt.Errorf("End of Input")

type JSONReader struct {
	scanner *bufio.Scanner
}

var NoMessage = fmt.Errorf("No message")

func NewJSONReader(reader io.ReadCloser) (*JSONReader, error) {
	return &JSONReader{bufio.NewScanner(reader)}, nil
}

func (e *JSONReader) ReadMsg() (RowMessage, error) {
	if e.scanner.Scan() {
		bytes := e.scanner.Bytes()
		row := make(JSONRowMessage)
		err := json.Unmarshal(bytes, &row)
		return row, err
	}
	err := e.scanner.Err()
	if err != nil {
		return nil, err
	}
	return nil, NoMessage
}

const MaxHekaMessageSize = 4194304 // cf. heka-cat -h

type HekaRowMessage struct {
	message.Message
}

func (m *HekaRowMessage) GetType() string {
	return *m.Type
}

func (m *HekaRowMessage) GetNumColumns() int {
	return len(m.Fields) + 1 // adds heka timestamp field
}

func (m *HekaRowMessage) GetColumn(index int) (string, interface{}) {
	if index == len(m.Fields) {
		return "timestamp", int(math.Floor(float64(*m.Timestamp) / 1e9))
	}

	// Heka message fields are slices. We only return the first value,
	// and only if that's the only value.
	f := m.Fields[index]
	switch f.GetValueType() {
	case message.Field_STRING:
		val := f.GetValueString()
		if len(val) == 1 {
			return f.GetName(), val[0]
		}

	case message.Field_BYTES:
		val := f.GetValueBytes()
		if len(val) == 1 {
			return f.GetName(), val[0]
		}

	case message.Field_INTEGER:
		val := f.GetValueInteger()
		if len(val) == 1 {
			return f.GetName(), val[0]
		}

	case message.Field_DOUBLE:
		val := f.GetValueDouble()
		if len(val) == 1 {
			return f.GetName(), val[0]
		}

	case message.Field_BOOL:
		val := f.GetValueBool()
		if len(val) == 1 {
			return f.GetName(), val[0]
		}
	}
	return "", nil
}

func (m *HekaRowMessage) GetValue(name string) interface{} {
	if name == "timestamp" {
		_, val := m.GetColumn(len(m.Fields))
		return val
	}

	for index, f := range m.Fields {
		if f.GetName() == name {
			_, val := m.GetColumn(index)
			return val
		}
	}
	return nil
}

type HekaReader struct {
	reader io.ReadCloser
	buf    []byte
}

func NewHekaReader(reader io.ReadCloser) (*HekaReader, error) {
	return &HekaReader{reader,
		make([]byte, MaxHekaMessageSize),
	}, nil
}

func (e *HekaReader) ReadMsg() (RowMessage, error) {
	// return nil, fmt.Errorf("Not implemented")
	numBytes, err := e.reader.Read(e.buf)
	if err != nil {
		return nil, err
	}
	rowMessage := &HekaRowMessage{}
	err = proto.Unmarshal(e.buf[:numBytes], rowMessage)
	return rowMessage, err
}

/////

// cf. https://hekad.readthedocs.org/en/latest/message/index.html#stream-framing
const MaxHekaStreamMessageSize = MaxHekaMessageSize + 3 + 255
const HekaStreamBufSize = MaxHekaStreamMessageSize * 50

type HekaStreamReader struct {
	reader       io.ReadCloser
	header       *message.Header
	buf          []byte
	bufStart     int
	bufEnd       int
	messageCount int
}

const (
	HSInit = iota
	HSHeader
	HSMessage
)

const HSHeaderOffset = 2

func NewHekaStreamReader(reader io.ReadCloser) (*HekaStreamReader, error) {
	return &HekaStreamReader{reader,
		&message.Header{},
		make([]byte, MaxHekaStreamMessageSize),
		0,
		0,
		0,
	}, nil
}

func (e *HekaStreamReader) ReadMsg() (RowMessage, error) {
	state := HSInit
	var readStart, readEnd int
	var enableLog bool

	bufLen := len(e.buf)

	for {
		if e.bufEnd > len(e.buf) {
			panic(fmt.Sprintf("e.bufEnd=%d len=%d", e.bufEnd, bufLen))
		}

		for {
			if enableLog {
				log.Printf("state=%d bufStart=%d bufEnd=%d readStart=%d readEnd=%d bufLen=%d",
					state, e.bufStart, e.bufEnd, readStart, readEnd, bufLen)
			}
			// log.Printf("buf+20: %x", e.buf[e.bufStart:e.bufStart+20])

			switch state {
			case HSInit:
				if e.bufEnd < e.bufStart+2 {
					break
				}
				if e.buf[e.bufStart] != '\x1e' {
					// log.Printf("Buf: %d %v", e.bufStart, e.buf)
					return nil, fmt.Errorf("Invalid byte at start of message: %#x", e.buf[e.bufStart])
				}
				readStart = e.bufStart + HSHeaderOffset
				readEnd = readStart + int(e.buf[e.bufStart+1])
				state = HSHeader
				continue

			case HSHeader:
				if e.bufEnd < readEnd {
					break
				}
				err := proto.Unmarshal(e.buf[readStart:readEnd], e.header)
				if err != nil {
					return nil, err
				}
				readStart = readEnd + 1
				readEnd = readStart + int(*e.header.MessageLength)
				state = HSMessage
				continue

			case HSMessage:
				if e.bufEnd < readEnd {
					break
				}
				if e.buf[readStart-1] != '\x1f' {
					return nil, fmt.Errorf("Invalid byte at start of message: %#x", e.buf[readStart-1])
				}
				e.bufStart = readEnd
				rowMessage := &HekaRowMessage{}
				err := proto.Unmarshal(e.buf[readStart:readEnd], rowMessage)
				e.messageCount++
				if e.messageCount%100 == 0 {
					log.Printf("Message count=%d", e.messageCount)
				}

				// log.Printf("Returning message: %v", e.rowMessage)
				return rowMessage, err
			}

			// Only read if we need to.
			var n int
			var err error
			if e.bufEnd == bufLen {
				n = 0
				err = nil
			} else {
				n, err = e.reader.Read(e.buf[e.bufEnd:])
				if err != nil {
					log.Printf("read: n=%d err=%v bufEnd=%d", n, err, e.bufEnd)
					if err != io.EOF {
						return nil, err
					}
				}
				if n == 0 && err == io.EOF {
					if state != HSInit {
						log.Printf("WARNING: EOF state=%d", state)
					}
					return nil, err
				}
			}

			if n == 0 {
				// circle back to the start of the buffer if more to read
				numBytes := len(e.buf) - e.bufStart
				numBytesCopied := copy(e.buf[0:numBytes], e.buf[e.bufStart:])
				if numBytes != numBytesCopied {
					return nil, fmt.Errorf("Internal error: copied  %d, not %d bytes",
						numBytesCopied, numBytes)
				}
				readEnd = (readStart - e.bufStart) + readEnd - readStart
				readStart = readStart - e.bufStart
				e.bufStart = 0
				e.bufEnd = numBytes
				enableLog = true
			} else {
				e.bufEnd += n
				enableLog = true
			}
		}
	}
}

func NewMessageReader(inputFormat string, reader io.ReadCloser) (MessageReader, error) {
	switch inputFormat {
	case "json":
		return NewJSONReader(reader)

	case "heka":
		return NewHekaReader(reader)

	case "heka-stream":
		return NewHekaStreamReader(reader)

	default:
		return nil, fmt.Errorf("Unsupported input format: %s", inputFormat)
	}
}
