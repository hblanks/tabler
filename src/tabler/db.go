package tabler

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"tabler/rowmessage"
	"tabler/tabledef"
)

func ConnectDB(dataSourceName string) (*sql.DB, string, error) {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return nil, "", err
	}

	var db *sql.DB
	switch u.Scheme {
	case "sqlite3", "sqlite":
		db, err = sql.Open("sqlite3", u.Path)
		return db, "sqlite3", err

	case "postgres", "postgresql":
		db, err = sql.Open("postgres", dataSourceName)
		return db, "postgres", err

	default:
		return nil, "", fmt.Errorf("Unsupported DSN")
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
