package metadata

import (
	"fmt"
	"testing"

	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/server"
	"github.com/stretchr/testify/require"
)

func TestTableManager(t *testing.T) {
	db := server.NewSimpleDB("data", 400, 8)
	tx, err := db.NewTransaction()
	require.NoError(t, err)
	tm, err := NewTableManager(true, tx)
	require.NoError(t, err)

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	err = tm.CreateTable("MyTable", schema, tx)
	require.NoError(t, err)

	layout, err := tm.Layout("MyTable", tx)
	require.NoError(t, err)
	size := layout.SlotSize()

	schema2 := layout.Schema()

	fmt.Printf("MyTable has slot size %d\n", size)
	fmt.Printf("fields are:\n")

	for _, fn := range schema2.Fields() {
		var strType string
		ftype, err := schema2.FieldType(fn)
		require.NoError(t, err)

		switch ftype {
		case record.Integer:
			strType = "int"
		case record.String:
			strType = "string"
		default:
		}

		fmt.Printf("%s: %s\n", fn, strType)
	}
	err = tx.Commit()
	require.NoError(t, err)
}
