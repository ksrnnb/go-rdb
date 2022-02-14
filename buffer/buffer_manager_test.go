package buffer_test

import (
	"fmt"
	"testing"

	. "github.com/ksrnnb/go-rdb/buffer"
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/server"
)

const filename = "testfile"

func newBufferManager(t *testing.T) *BufferManager {
	t.Helper()
	db := server.NewSimpleDB("data", 400, 3)
	return db.BufferManager()
}

func blockID(t *testing.T, filename string, blkNum int) *file.BlockID {
	t.Helper()
	return file.NewBlockID(filename, blkNum)
}

func expectNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("err == nil expected, but %v", err)
	}
}

// 接続できないことを確かめるので、10秒かかる
func TestPin(t *testing.T) {
	// バッファサイズは3
	bm := newBufferManager(t)

	buffers := make([]*Buffer, 10)

	var err error
	buffers[0], err = bm.Pin(blockID(t, filename, 0))
	expectNoError(t, err)
	buffers[1], err = bm.Pin(blockID(t, filename, 1))
	expectNoError(t, err)
	buffers[2], err = bm.Pin(blockID(t, filename, 2))
	expectNoError(t, err)

	bm.Unpin(buffers[1])
	buffers[1] = nil

	// 0番はpinされたままなので、numAvailableは増えない
	buffers[3], err = bm.Pin(blockID(t, filename, 0))
	expectNoError(t, err)
	buffers[4], err = bm.Pin(blockID(t, filename, 1))
	expectNoError(t, err)

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
	expectNoError(t, err)
	buffers[1], err = bm.Pin(blockID(t, filename, 1))
	expectNoError(t, err)
	buffers[2], err = bm.Pin(blockID(t, filename, 2))
	expectNoError(t, err)

	// ここで10秒かかる
	buffers[5], err = bm.Pin(blockID(t, filename, 3))
	if err == nil {
		t.Errorf("bm.Pin() should return error but get nil")
	}
}
