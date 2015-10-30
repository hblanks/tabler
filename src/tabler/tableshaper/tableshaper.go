package tableshaper

import (
	"encoding/json"
	"fmt"

	"tabler/rowmessage"
	"tabler/tabledef"
)

const INIT_TABLEDEF_COUNT = 50

type TableShaper struct {
	tables map[string]*tabledef.TableDef
}

func NewTableShaper() *TableShaper {
	return &TableShaper{tables: make(map[string]*tabledef.TableDef)}
}

func setColumnDef(columnDef *tabledef.ColumnDef, name string, value interface{}) error {
	columnDef.Name = name
	switch value.(type) {
	case string:
		columnDef.Ctype = tabledef.ColumnTextType
	case int, int64:
		columnDef.Ctype = tabledef.ColumnIntegerType
	case float32, float64:
		columnDef.Ctype = tabledef.ColumnFloatType
	case bool:
		columnDef.Ctype = tabledef.ColumnBooleanType
	default:
		return fmt.Errorf("Unknown column type %v", value)
	}
	return nil
}

func newTableDef(name string, row rowmessage.RowMessage) (*tabledef.TableDef, error) {
	var err error
	var columnDefs []tabledef.ColumnDef

	switch r := row.(type) {
	case rowmessage.SliceRowMessage:
		numCols := r.GetNumColumns()
		columnDefs = make([]tabledef.ColumnDef, numCols)
		for i := 0; i < numCols; i++ {
			key, value := r.GetColumn(i)
			err = setColumnDef(&columnDefs[i], key, value)
			if err != nil {
				return nil, err
			}
		}

	case rowmessage.MapRowMessage:
		columns := r.GetMap()
		columnDefs = make([]tabledef.ColumnDef, len(columns))
		i := 0
		for key, value := range columns {
			err = setColumnDef(&columnDefs[i], key, value)
			if err != nil {
				return nil, err
			}
			i++
		}

	default:
		return nil, fmt.Errorf("Unknown type %v", r)
	}

	return &tabledef.TableDef{Name: name, Columns: columnDefs}, nil

}

// func updateTableDef(tableDef *tabledef.TableDef, row rowmessage.RowMessage) error {
//     var err error
//     // var columnDefs[]tabledef.ColumnDef

//     switch r := row.(type) {
//     case rowmessage.SliceRowMessage:
//         numCols := r.GetNumColumns()
//         // columnDefs = make([]tabledef.ColumnDef, numCols)
//         for i := 0; i < numCols; i++ {
//             key, value := r.GetColumn(i)
//             err = setColumnDef(&columnDefs[i], key, value)
//             fmt.Printf("cdef: %v\n", columnDefs[i])
//             if err != nil {
//                 return nil, err
//             }
//         }

//     case rowmessage.MapRowMessage:
//         columns := r.GetMap()
//         columnDefs = make([]tabledef.ColumnDef, len(columns))
//         i := 0
//         for key, value := range columns {
//             err = setColumnDef(&columnDefs[i], key, value)
//             if err != nil {
//                 return nil, err
//             }
//             i++
//         }

//     default:
//         return nil, fmt.Errorf("Unknown type %v", r)
//     }
// }

func (t *TableShaper) Add(row rowmessage.RowMessage) error {
	var err error
	messageType := row.GetType()
	tableDef := t.tables[messageType]
	if tableDef == nil {
		tableDef, err = newTableDef(messageType, row)
		if err != nil {
			return err
		}
		t.tables[messageType] = tableDef
	}

	// TODO: merge...as much as we need.
	// TODO: set notnull = True as needed?

	// err = updateTableDef(tableDef, row)
	// return err
	return nil
}

func (t *TableShaper) GetTablesJSON() ([]byte, error) {
	return json.MarshalIndent(t.tables, "", "    ")
}
