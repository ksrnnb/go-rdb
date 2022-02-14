package buffer_test

import (
	"fmt"
	"testing"
)

func TestBuffer(t *testing.T) {
	bm := newBufferManager(t)

	buff1, err := bm.Pin(blockID(t, filename, 1))
	expectNoError(t, err)

	p := buff1.Contents()

	n, err := p.GetInt(80)
	expectNoError(t, err)

	err = p.SetInt(80, n+1)
	expectNoError(t, err)
	buff1.SetModified(1, 0)

	// テスト実行のたびに値はインクリメントされる
	fmt.Printf("the new value is %d\n", n+1)

	bm.Unpin(buff1)

	// one of these pins will flush buff1 to disk
	buff2, err := bm.Pin(blockID(t, filename, 2))
	expectNoError(t, err)
	_, err = bm.Pin(blockID(t, filename, 3))
	expectNoError(t, err)
	_, err = bm.Pin(blockID(t, filename, 4))
	expectNoError(t, err)

	bm.Unpin(buff2)
	buff2, err = bm.Pin(blockID(t, filename, 1))
	expectNoError(t, err)

	p2 := buff2.Contents()
	err = p2.SetInt(80, 9999)
	expectNoError(t, err)

	buff2.SetModified(1, 0)
	bm.Unpin(buff2)
}
