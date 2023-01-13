package concurrency_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ksrnnb/go-rdb/buffer"
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
	myTesting "github.com/ksrnnb/go-rdb/testing"
	"github.com/ksrnnb/go-rdb/tx"
	"github.com/ksrnnb/go-rdb/tx/concurrency"
	"github.com/stretchr/testify/require"
)

func TestConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	sdb := myTesting.NewSimpleDB(t, "data", 400, 8)
	fm := sdb.FileManager()
	lm := sdb.LogManager()
	bm := sdb.BufferManager()
	lt := concurrency.NewLockTable()
	tng := tx.NewTransactionNumberGenerator()

	testFunc1 := func(t *testing.T, ch chan<- struct{}, fm *file.FileManager, lm *logs.LogManager, bm *buffer.BufferManager) {
		defer wg.Done()
		tx, err := tx.NewTransaction(fm, lm, bm, lt, tng)
		require.NoError(t, err)
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		require.NoError(t, tx.Pin(blk1))
		require.NoError(t, tx.Pin(blk2))

		// slock 1
		fmt.Println("Tx 1: request slock 1")
		_, err = tx.GetInt(blk1, 0)
		require.NoError(t, err)
		fmt.Println("Tx 1: receive slock 1")
		ch <- struct{}{}

		time.Sleep(1 * time.Second)

		// slcok 2
		fmt.Println("Tx 1: request slock 2")
		_, err = tx.GetInt(blk2, 0)
		require.NoError(t, err)
		fmt.Println("Tx 1: receive slock 2")

		require.NoError(t, tx.Commit())
		fmt.Println("Tx 1: Commit done")
	}

	testFunc2 := func(t *testing.T, fm *file.FileManager, lm *logs.LogManager, bm *buffer.BufferManager) {
		defer wg.Done()

		tx, err := tx.NewTransaction(fm, lm, bm, lt, tng)
		require.NoError(t, err)
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		require.NoError(t, tx.Pin(blk1))
		require.NoError(t, tx.Pin(blk2))

		// xlock 2
		fmt.Println("Tx 2: request xlock 2")
		require.NoError(t, tx.SetInt(blk2, 0, 0, false))
		require.NoError(t, err)
		fmt.Println("Tx 2: receive xlock 2")

		time.Sleep(1 * time.Second)

		// slcok 1
		fmt.Println("Tx 2: request slock 1")
		_, err = tx.GetInt(blk1, 0)
		require.NoError(t, err)
		fmt.Println("Tx 2: receive slock 1")

		require.NoError(t, tx.Commit())
		fmt.Println("Tx 2: Commit done")

	}

	testFunc3 := func(t *testing.T, ch <-chan struct{}, fm *file.FileManager, lm *logs.LogManager, bm *buffer.BufferManager) {
		defer wg.Done()

		// func1 で slock するまで待つ
		<-ch
		tx, err := tx.NewTransaction(fm, lm, bm, lt, tng)
		require.NoError(t, err)
		blk1 := file.NewBlockID("testfile", 1)
		blk2 := file.NewBlockID("testfile", 2)
		require.NoError(t, tx.Pin(blk1))
		require.NoError(t, tx.Pin(blk2))
		// xlock 1
		// func 1 が Commit するまで SetInt は完了しない
		fmt.Println("Tx 3: request xlock 1")
		require.NoError(t, tx.SetInt(blk1, 0, 0, false))
		fmt.Println("Tx 3: receive xlock 1")
		require.NoError(t, err)

		time.Sleep(1 * time.Second)

		// slcok 2
		fmt.Println("Tx 3: request slock 2")
		_, err = tx.GetInt(blk2, 0)
		require.NoError(t, err)
		fmt.Println("Tx 3: receive slock 2")

		require.NoError(t, tx.Commit())
		fmt.Println("Tx 3: Commit done")
	}

	wg.Add(3)
	ch := make(chan struct{})
	go testFunc1(t, ch, fm, lm, bm)
	go testFunc2(t, fm, lm, bm)
	go testFunc3(t, ch, fm, lm, bm)

	wg.Wait()
}
