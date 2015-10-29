package tabledef

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"tabler/rowmessage"
)

const MaxJSONLength = 1024 * 1024

const (
	ColumnFloatType   = "float"
	ColumnIntegerType = "integer"
	ColumnTextType    = "text"
	ColumnBooleanType = "boolean"
)

type ColumnDef struct {
	Name  string `json:"name"`
	Ctype string `json:"type"`
	// primaryKey bool
	NotNull bool `json:"notNull"`
}

func getColumnDataType(ctype string) string {
	// only SQLite3 types supported for now.
	switch ctype {
	case ColumnFloatType:
		return "float"
	case ColumnIntegerType, ColumnBooleanType:
		return "integer"
	case ColumnTextType:
		return "text"
	default:
		return ""
	}
}

func (t *ColumnDef) getCreateSQL() string {
	return fmt.Sprintf("\"%s\" %s", t.Name, getColumnDataType(t.Ctype))
}

type TableDef struct {
	Name      string      `json:"name"`
	Columns   []ColumnDef `json:"columns"`
	Ignore    bool        `json:"ignore"`
	insertSQL string
}

func (t *TableDef) GetCreateSQL() string {
	columnExprs := make([]string, len(t.Columns))
	for index, column := range t.Columns {
		columnExprs[index] = column.getCreateSQL()
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (%s)",
		t.Name, strings.Join(columnExprs, ", "))
}

func postgresValues(count int) string {
	values := make([]string, count)
	for i := 0; i < count; i++ {
		values[i] = "$" + strconv.Itoa(i+1)
	}
	return strings.Join(values, ", ")
}

func (t *TableDef) setInsertSQL(driverName string) error {
	var valuesStr string
	switch driverName {
	case "sqlite3":
		valuesStr = strings.Join(strings.Split(strings.Repeat("?", len(t.Columns)), ""), ", ")
	case "postgres":
		valuesStr = postgresValues(len(t.Columns))
	default:
		return fmt.Errorf("TableDef.setInsertSQL: unsupported driver=%s", driverName)
	}
	t.insertSQL = fmt.Sprintf("INSERT INTO \"%s\" VALUES (%s)", t.Name, valuesStr)
	return nil
}

func (t *TableDef) Insert(db *sql.DB, row rowmessage.RowMessage) error {
	args := make([]interface{}, len(t.Columns))
	for index, column := range t.Columns {
		args[index] = row.GetValue(column.Name)
		if args[index] == nil && column.NotNull {
			return fmt.Errorf("Missing column %s: %s", column.Name, row)
		}
	}
	_, err := db.Exec(t.insertSQL, args...)
	if err != nil {
		log.Printf("TableDef.insert: error sql=\"%s\" args=%v", t.insertSQL, args)
	}
	return err
}

type TableDefs map[string]*TableDef

func ReadTablesJSON(path string) (TableDefs, error) {
	file, err := os.Open(path)
	defer file.Close()

	buf := make([]byte, MaxJSONLength)
	n, err := file.Read(buf)
	if err != nil {
		return nil, err
	}

	tables := make(map[string]*TableDef)
	err = json.Unmarshal(buf[0:n], &tables)
	if err != nil {
		return nil, err
	}

	for name, table := range tables {
		if table.Name == "" {
			table.Name = name
		}
	}

	return tables, nil
}

func SetSQL(tables TableDefs, driverName string) error {
	for _, table := range tables {
		if !table.Ignore {
			err := table.setInsertSQL(driverName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
