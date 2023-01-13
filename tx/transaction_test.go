package tx

import (
	"testing"

	"github.com/ksrnnb/go-rdb/file"
	myTesting "github.com/ksrnnb/go-rdb/testing"
	"github.com/ksrnnb/go-rdb/tx/concurrency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransaction(t *testing.T) {
	sdb := myTesting.NewSimpleDB(t, "data", 400, 8)
	fm := sdb.FileManager()
	lm := sdb.LogManager()
	bm := sdb.BufferManager()
	lt := concurrency.NewLockTable()
	tng := NewTransactionNumberGenerator()

	tx1, err := NewTransaction(fm, lm, bm, lt, tng)
	require.NoError(t, err)

	blk := file.NewBlockID("testfile", 1)
	require.NoError(t, tx1.Pin(blk))
	require.NoError(t, tx1.SetInt(blk, 80, 1, false))
	require.NoError(t, tx1.SetString(blk, 40, "one", false))
	require.NoError(t, tx1.Commit())

	tx2, err := NewTransaction(fm, lm, bm, lt, tng)
	require.NoError(t, err)
	require.NoError(t, tx2.Pin(blk))
	intVal, err := tx2.GetInt(blk, 80)
	require.NoError(t, err)
	assert.Equal(t, 1, intVal, "get int value")
	strVal, err := tx2.GetString(blk, 40)
	require.NoError(t, err)
	assert.Equal(t, "one", strVal, "get string value")

	newIntVal := intVal + 1
	newStrVal := strVal + "!"

	require.NoError(t, tx2.SetInt(blk, 80, newIntVal, true))
	require.NoError(t, tx2.SetString(blk, 40, newStrVal, true))
	require.NoError(t, tx2.Commit())

	tx3, err := NewTransaction(fm, lm, bm, lt, tng)
	require.NoError(t, err)
	require.NoError(t, tx3.Pin(blk))
	intVal, err = tx3.GetInt(blk, 80)
	require.NoError(t, err)
	assert.Equal(t, 2, intVal, "get int value")
	strVal, err = tx3.GetString(blk, 40)
	require.NoError(t, err)
	assert.Equal(t, "one!", strVal, "get string value")

	require.NoError(t, tx3.SetInt(blk, 80, 9999, true))
	intVal, err = tx3.GetInt(blk, 80)
	require.NoError(t, err)
	assert.Equal(t, 9999, intVal, "get int value")
	require.NoError(t, tx3.Rollback())

	tx4, err := NewTransaction(fm, lm, bm, lt, tng)
	require.NoError(t, err)
	require.NoError(t, tx4.Pin(blk))
	intVal, err = tx4.GetInt(blk, 80)
	require.NoError(t, err)
	assert.Equal(t, 2, intVal, "get int value")
	require.NoError(t, tx4.Commit())
}
