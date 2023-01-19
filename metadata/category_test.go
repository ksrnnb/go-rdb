package metadata

import (
	"fmt"
	"testing"

	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/server"
	"github.com/stretchr/testify/require"
)

func TestCatalog(t *testing.T) {
	db := server.NewSimpleDB("data", 400, 8)
	tx, err := db.NewTransaction()
	require.NoError(t, err)
	tm, err := NewTableManager(true, tx)
	require.NoError(t, err)

	layout, err := tm.Layout(tableCategoryTableName, tx)
	require.NoError(t, err)
	ts, err := record.NewTableScan(tx, tableCategoryTableName, layout)
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
	fts, err := record.NewTableScan(tx, fieldCategoryTableName, flayout)
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
