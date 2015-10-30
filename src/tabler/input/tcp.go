package input

import (
	"io"
	"log"
	"net"
	"tabler/rowmessage"
	"time"
)

//
// TCPInput
//

const TCPInputChanSize = 1000
const TCPInputTimeout = 60 * 1e9 // 60s
const TCPMaxErrors = 30

type TCPInput struct {
	addr        *net.TCPAddr
	inputFormat string
	listener    *net.TCPListener
	messages    chan rowmessage.RowMessage
	// exit chan nil
}

func NewTCPInput(addressString string, inputFormat string) (*TCPInput, error) {
	addr, err := net.ResolveTCPAddr("tcp", addressString)
	if err != nil {
		return nil, err
	}
	t := &TCPInput{addr,
		inputFormat,
		nil,
		make(chan rowmessage.RowMessage, TCPInputChanSize)}
	return t, nil
}

func (t *TCPInput) Init() error {
	listener, err := net.ListenTCP("tcp", t.addr)
	if err != nil {
		log.Printf("listenTCP: listen error=%s", err)
		close(t.messages)
		return err
	}
	t.listener = listener
	go t.listenTCP()
	return nil
}

func (t *TCPInput) listenTCP() {
	for {
		conn, err := t.listener.AcceptTCP()
		if err != nil {
			log.Printf("listenTCP: accept error=%s", err)
			return
		}
		log.Printf("TCPInput.listenTCP: conn=%s", conn)
		messageReader, err := rowmessage.NewMessageReader(t.inputFormat, conn)
		if err != nil {
			conn.Close()
			log.Printf("listenTCP: NewMessageReader error=%s", err)
			return
		}
		go t.receiveMessages(conn, messageReader)
	}
}

func (t *TCPInput) receiveMessages(conn *net.TCPConn, messageReader rowmessage.MessageReader) {
	errCount := 0
	msgCount := 0
	defer conn.Close()
	defer func() {
		log.Printf("TCPInput.receiveMessages: total=%d", msgCount)
	}()

	for {
		conn.SetDeadline(time.Now().Add(TCPInputTimeout))
		msg, err := messageReader.ReadMsg()
		if err != nil {
			if err == io.EOF {
				log.Printf("TCPInput.receiveMessages: EOF!")
				return
			}

			netErr, ok := err.(net.Error)
			if ok {
				if netErr.Timeout() && !netErr.Temporary() {
					log.Printf("TCPInput.receiveMessages: timeout conn=%v", conn)
					return
				}
				errCount++
				if errCount > TCPMaxErrors {
					log.Printf("TCPInput.receiveMessages: max errors reached err=%s", msg, err)
					return
				}
			}

			log.Printf("TCPInput.receiveMessages: error=%s", err)
			continue
		}
		errCount = 0
		// log.Printf("TCPInput.receiveMessages: msg=%v err=%s", msg, err)
		t.messages <- msg
		msgCount++
	}
}

func (t *TCPInput) ReadMsg() (rowmessage.RowMessage, error) {
	msg, ok := <-t.messages
	if ok {
		return msg, nil
	} else {
		return nil, rowmessage.EndOfInput
	}
}

func (t *TCPInput) Close() error {
	return t.listener.Close()
}
