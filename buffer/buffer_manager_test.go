package buffer_test

import (
	"fmt"
	"sync"
	"testing"

	. "github.com/ksrnnb/go-rdb/buffer"
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const filename = "tempTestFile"

func newBufferManager(t *testing.T) *BufferManager {
	t.Helper()
	db := server.NewSimpleDB("data", 400, 3)
	return db.BufferManager()
}

func blockID(t *testing.T, filename string, blkNum int) file.BlockID {
	t.Helper()
	return file.NewBlockID(filename, blkNum)
}

func TestPin(t *testing.T) {
	// バッファサイズは3
	bm := newBufferManager(t)

	buffers := make([]*Buffer, 10)

	var err error
	buffers[0], err = bm.Pin(blockID(t, filename, 0))
	require.NoError(t, err)
	buffers[1], err = bm.Pin(blockID(t, filename, 1))
	require.NoError(t, err)
	buffers[2], err = bm.Pin(blockID(t, filename, 2))
	require.NoError(t, err)
	assert.Equal(t, 0, bm.Available())

	bm.Unpin(buffers[1])
	buffers[1] = nil
	assert.Equal(t, 1, bm.Available())

	// 0番はpinされたままなので、numAvailableは変化しない
	buffers[3], err = bm.Pin(blockID(t, filename, 0))
	require.NoError(t, err)
	assert.Equal(t, 1, bm.Available())

	buffers[4], err = bm.Pin(blockID(t, filename, 1))
	require.NoError(t, err)
	assert.Equal(t, 0, bm.Available())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		buffers[4], err = bm.Pin(blockID(t, filename, 3))
		require.NoError(t, err)
		wg.Done()
	}()

	// unpin することで空きができて、blockID 3 の pin が処理を再開する
	bm.Unpin(buffers[4])
	buffers[4] = nil
	wg.Wait()

	assert.Equal(t, 0, bm.Available())

	for i := 0; i < len(buffers); i++ {
		b := buffers[i]
		if b != nil {
			fmt.Printf("i: %d, buff[%s] pinned to block\n", i, b.Block())
		}
	}
}

// pinできないことを確かめるため10秒かかる
// skipしておく
func TestCannotPinOverBufferSize(t *testing.T) {
	t.Skip()

	// バッファサイズは3
	bm := newBufferManager(t)

	buffers := make([]*Buffer, 10)

	var err error
	buffers[0], err = bm.Pin(blockID(t, filename, 0))
	require.NoError(t, err)
	buffers[1], err = bm.Pin(blockID(t, filename, 1))
	require.NoError(t, err)
	buffers[2], err = bm.Pin(blockID(t, filename, 2))
	require.NoError(t, err)

	// ここで10秒かかる
	buffers[5], err = bm.Pin(blockID(t, filename, 3))
	assert.Error(t, err)
}
