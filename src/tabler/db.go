package tabler

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"

	_ "github.com/mattn/go-sqlite3"

	"tabler/rowmessage"
	"tabler/tabledef"
)

func ConnectDB(dataSourceName string) (*sql.DB, error) {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "sqlite3", "sqlite":
		return sql.Open("sqlite3", u.Path)

	default:
		return nil, fmt.Errorf("Unsupported DSN")
	}
}

func CreateTables(db *sql.DB, tables tabledef.TableDefs) error {
	for _, table := range tables {
		if !table.Ignore {
			createSQL := table.GetCreateSQL()
			log.Printf("CreateTables: %s", createSQL)
			_, err := db.Exec(createSQL)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func Insert(db *sql.DB, tables tabledef.TableDefs, row rowmessage.RowMessage) error {
	tableName := row.GetType()
	if tableName == "" {
		return fmt.Errorf("Message had invalid or missing type property: %s", row)
	}

	table := tables[tableName]
	if table == nil {
		return fmt.Errorf("Message type not found in table list: %s", tableName)
	}
	if table.Ignore {
		return nil
	}
	return table.Insert(db, row)
}
