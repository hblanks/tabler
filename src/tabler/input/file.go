package input

import (
	"io"
	"os"
	"tabler/rowmessage"
)

type FileInput struct {
	file          *os.File
	messageReader rowmessage.MessageReader
	emitted       bool
}

func NewFileInput(file *os.File, inputFormat string) (*FileInput, error) {
	reader, err := rowmessage.NewMessageReader(inputFormat, file)
	if err != nil {
		return nil, err
	}
	return &FileInput{file, reader, false}, err
}

func (f *FileInput) Init() error {
	return nil
}

func (f *FileInput) ReadMsg() (rowmessage.RowMessage, error) {
	msg, err := f.messageReader.ReadMsg()
	if err == io.EOF {
		return nil, rowmessage.EndOfInput
	}
	return msg, err
}

func (f *FileInput) Close() error {
	return f.file.Close()
}
