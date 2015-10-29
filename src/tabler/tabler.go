package tabler

import (
	"database/sql"
	"log"
	"os"
	"sync"
	"tabler/input"
	"tabler/rowmessage"
	"tabler/tabledef"
	"tabler/tableshaper"
)

type MessageInput interface {
	ReadMsg() (rowmessage.RowMessage, error)
	Init() error
	Close() error
}

type Tabler struct {
	mutex        *sync.Mutex
	messageInput MessageInput

	db *sql.DB
}

func NewTabler() *Tabler {
	return &Tabler{mutex: &sync.Mutex{}}
}

func (t *Tabler) Init(listenSocket string, inputFormat string) error {
	var err error

	t.mutex.Lock()
	if listenSocket == "" {
		t.messageInput, err = input.NewFileInput(os.Stdin, inputFormat)
	} else {
		t.messageInput, err = input.NewTCPInput(listenSocket, inputFormat)
	}
	if err == nil {
		err = t.messageInput.Init()
	}
	t.mutex.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (t *Tabler) GenerateTables() ([]byte, error) {
	tableShaper := tableshaper.NewTableShaper()
	for {
		row, err := t.messageInput.ReadMsg()
		if err != nil {
			if err == rowmessage.EndOfInput {
				break
			}
			log.Printf("Tabler.GenerateTables: error=%s", err)
			return nil, err
		}

		err = tableShaper.Add(row)
		if err != nil {
			log.Printf("Tabler.GenerateTables: error=%v row=%v", err, row)
		}
	}
	return tableShaper.GetTablesJSON()
}

func (t *Tabler) WriteRows(tablesPath string, dsn string) error {
	tables, err := tabledef.ReadTablesJSON(tablesPath)
	if err != nil {
		log.Printf("Tabler.WriteRows: path=%s error=%s", tablesPath, err)
		return err
	}
	log.Printf("Tabler.WriteRows: tables=%v\n", tables)

	var driverName string
	t.mutex.Lock()
	t.db, driverName, err = ConnectDB(dsn)
	t.mutex.Unlock()
	if err != nil {
		return err
	}

	tabledef.SetSQL(tables, driverName)

	err = CreateTables(t.db, tables)
	if err != nil {
		return err
	}

	// count := 0
	for {
		row, err := t.messageInput.ReadMsg()
		if err != nil {
			if err == rowmessage.EndOfInput {
				break
			}
			log.Printf("Tabler.WriteRows: error=%s", err)
			return err
		}

		err = Insert(t.db, tables, row)
		if err != nil {
			log.Printf("Tabler.WriteRows: error=%v row=%v", err, row)
		}
		// count++
	}
	return nil
}

func (t *Tabler) Close() {
	t.mutex.Lock()
	if t.messageInput != nil {
		t.messageInput.Close()
	}
	if t.db != nil {
		t.db.Close()
	}
	t.mutex.Unlock()
}
