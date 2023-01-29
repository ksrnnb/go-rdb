package metadata_test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func initializeFiles(t *testing.T) {
	t.Helper()
	err := os.RemoveAll("../data")

	require.NoError(t, err)
}

func TestMetadataManager(t *testing.T) {
	initializeFiles(t)

	db := server.NewSimpleDB("data", 400, 8)
	tx, err := db.NewTransaction()
	require.NoError(t, err)
	mm, err := metadata.NewMetadataManager(true, tx)
	require.NoError(t, err)

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	// Part1 Table Metadata
	err = mm.CreateTable("MyTable", schema, tx)
	require.NoError(t, err)

	layout, err := mm.Layout("MyTable", tx)
	require.NoError(t, err)
	size := layout.SlotSize()
	schema2 := layout.Schema()

	fmt.Printf("MyTable has slot size %d\n", size)
	fmt.Printf("fields are:\n")

	for _, fn := range schema2.Fields() {
		ft, err := schema2.FieldType(fn)
		require.NoError(t, err)
		var ty string
		switch ft {
		case record.Integer:
			ty = "int"
		case record.String:
			l, err := schema2.Length(fn)
			require.NoError(t, err)
			ty = fmt.Sprintf("varchar(%d)", l)
		}
		fmt.Printf("%s: %s\n", fn, ty)
	}

	// Part2 Statistics Metadata
	ts, err := query.NewTableScan(tx, "MyTable", layout)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		err = ts.Insert()
		require.NoError(t, err)

		n := rand.Intn(50)
		err = ts.SetInt("A", n)
		require.NoError(t, err)
		err = ts.SetString("B", fmt.Sprintf("rec%d", n))
		require.NoError(t, err)
	}

	si, err := mm.GetStatInfo("MyTable", layout, tx)
	require.NoError(t, err)

	fmt.Printf("B(MyTable) = %d\n", si.BlocksAccessed())
	fmt.Printf("R(MyTable) = %d\n", si.RecordsOutput())
	fmt.Printf("V(MyTable, A) = %d\n", si.DistinctValues("A"))
	fmt.Printf("V(MyTable, B) = %d\n", si.DistinctValues("B"))

	err = tx.Commit()
	require.NoError(t, err)

	// Part3: View Metadata
	definition := "select B from MyTable where A = 1"
	err = mm.CreateView("viewA", definition, tx)

	require.NoError(t, err)
	gotDef, err := mm.GetViewDefinition("viewA", tx)

	require.NoError(t, err)
	assert.Equal(t, definition, gotDef)

	// Part4: Index Metadata
	err = mm.CreateIndex("indexA", "MyTable", "A", tx)

	require.NoError(t, err)
	err = mm.CreateIndex("indexB", "MyTable", "B", tx)
	require.NoError(t, err)

	iis, err := mm.GetIndexInfo("MyTable", tx)
	require.NoError(t, err)

	ii, ok := iis["A"]
	require.True(t, ok)
	fmt.Printf("B(indexA) = %d\n", ii.BlocksAccessed())
	fmt.Printf("R(indexA) = %d\n", ii.RecordsOutput())
	fmt.Printf("V(indexA, A) = %d\n", ii.DistinctValues("A"))
	fmt.Printf("V(indexA, B) = %d\n", ii.DistinctValues("B"))

	ii, ok = iis["B"]
	require.True(t, ok)
	fmt.Printf("B(indexB) = %d\n", ii.BlocksAccessed())
	fmt.Printf("R(indexB) = %d\n", ii.RecordsOutput())
	fmt.Printf("V(indexB, A) = %d\n", ii.DistinctValues("A"))
	fmt.Printf("V(indexB, B) = %d\n", ii.DistinctValues("B"))
}
