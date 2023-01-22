package metadata_test

import (
	"fmt"
	"testing"

	"github.com/ksrnnb/go-rdb/metadata"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/server"
	"github.com/stretchr/testify/require"
)

const (
	tableCategoryTableName = "table_categories"
	fieldCategoryTableName = "field_categories"
)

const (
	tableNameField = "table_name"
	slotSizeField  = "slot_size"
	fieldNameField = "field_name"
	fieldTypeField = "field_type"
	lengthField    = "length"
	offsetField    = "offset"
)

func TestCatalog(t *testing.T) {
	initializeFiles(t)

	db := server.NewSimpleDB("data", 400, 8)
	tx, err := db.NewTransaction()
	require.NoError(t, err)
	tm, err := metadata.NewTableManager(true, tx)
	require.NoError(t, err)

	layout, err := tm.Layout(tableCategoryTableName, tx)
	require.NoError(t, err)
	ts, err := query.NewTableScan(tx, tableCategoryTableName, layout)
	require.NoError(t, err)

	hasNext, err := ts.Next()
	require.NoError(t, err)
	for hasNext {
		tn, err := ts.GetString(tableNameField)
		require.NoError(t, err)
		size, err := ts.GetInt(slotSizeField)
		require.NoError(t, err)

		fmt.Printf("%s size: %d\n", tn, size)
		newHasNext, err := ts.Next()
		require.NoError(t, err)
		hasNext = newHasNext
	}
	err = ts.Close()
	require.NoError(t, err)

	flayout, err := tm.Layout(fieldCategoryTableName, tx)
	require.NoError(t, err)
	fts, err := query.NewTableScan(tx, fieldCategoryTableName, flayout)
	require.NoError(t, err)

	hasNext, err = fts.Next()
	require.NoError(t, err)
	for hasNext {
		tn, err := fts.GetString(tableNameField)
		require.NoError(t, err)
		fn, err := fts.GetString(fieldNameField)
		require.NoError(t, err)
		offset, err := fts.GetInt(offsetField)
		require.NoError(t, err)

		fmt.Printf("%s %s offset: %d\n", tn, fn, offset)

		newHasNext, err := fts.Next()
		require.NoError(t, err)
		hasNext = newHasNext
	}

	err = fts.Close()
	require.NoError(t, err)
}
