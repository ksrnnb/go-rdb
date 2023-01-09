package buffer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	bm := newBufferManager(t)

	buff1, err := bm.Pin(blockID(t, filename, 1))
	require.NoError(t, err)

	p := buff1.Contents()

	n, err := p.GetInt(80)
	require.NoError(t, err)

	err = p.SetInt(80, n+1)
	require.NoError(t, err)
	buff1.SetModified(1, 0)

	// テスト実行のたびに値はインクリメントされる
	fmt.Printf("the new value is %d\n", n+1)

	bm.Unpin(buff1)

	// one of these pins will flush buff1 to disk
	buff2, err := bm.Pin(blockID(t, filename, 2))
	require.NoError(t, err)
	_, err = bm.Pin(blockID(t, filename, 3))
	require.NoError(t, err)
	_, err = bm.Pin(blockID(t, filename, 4))
	require.NoError(t, err)

	// こっちはflushされないのでディスクには書き込まれない
	bm.Unpin(buff2)
	buff2, err = bm.Pin(blockID(t, filename, 1))
	require.NoError(t, err)

	p2 := buff2.Contents()
	err = p2.SetInt(80, 9999)
	require.NoError(t, err)

	buff2.SetModified(1, 0)
	bm.Unpin(buff2)
}
