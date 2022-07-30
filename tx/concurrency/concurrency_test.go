package concurrency_test

import (
	"sync"
	"testing"
	"time"

	"github.com/ksrnnb/go-rdb/buffer"
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
	myTesting "github.com/ksrnnb/go-rdb/testing"
	"github.com/ksrnnb/go-rdb/tx"
	"github.com/stretchr/testify/require"
)

func TestConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	sdb := myTesting.NewSimpleDB(t, "data", 400, 8)
	fm := sdb.FileManager()
	lm := sdb.LogManager()
	bm := sdb.BufferManager()

	testFunc1 := func(t *testing.T, fm *file.FileManager, lm *logs.LogManager, bm *buffer.BufferManager) {
		defer wg.Done()
		tx, err := tx.NewTransaction(fm, lm, bm)
		require.NoError(t, err)
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		require.NoError(t, tx.Pin(blk1))
		require.NoError(t, tx.Pin(blk2))

		// slock 1
		_, err = tx.GetInt(blk1, 0)
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)

		// slcok 2
		_, err = tx.GetInt(blk2, 0)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	testFunc2 := func(t *testing.T, fm *file.FileManager, lm *logs.LogManager, bm *buffer.BufferManager) {
		defer wg.Done()
		tx, err := tx.NewTransaction(fm, lm, bm)
		require.NoError(t, err)
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		require.NoError(t, tx.Pin(blk1))
		require.NoError(t, tx.Pin(blk2))

		// xlock 2
		require.NoError(t, tx.SetInt(blk2, 0, 0, false))
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)

		// slcok 1
		_, err = tx.GetInt(blk1, 0)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	testFunc3 := func(t *testing.T, fm *file.FileManager, lm *logs.LogManager, bm *buffer.BufferManager) {
		defer wg.Done()
		tx, err := tx.NewTransaction(fm, lm, bm)
		require.NoError(t, err)
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		require.NoError(t, tx.Pin(blk1))
		require.NoError(t, tx.Pin(blk2))

		// xlock 1
		require.NoError(t, tx.SetInt(blk1, 0, 0, false))
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)

		// slcok 2
		_, err = tx.GetInt(blk2, 0)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	wg.Add(3)
	go testFunc1(t, fm, lm, bm)
	go testFunc2(t, fm, lm, bm)
	go testFunc3(t, fm, lm, bm)

	wg.Wait()
}
