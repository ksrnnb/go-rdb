package logs

import (
	"fmt"
	"testing"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createRecords(t *testing.T, lm *LogManager, start, end int) {
	t.Helper()

	for i := start; i <= end; i++ {
		s := fmt.Sprintf("record%d", i)
		npos := file.MaxLengthInString(s)

		// 文字列長（整数）, 文字列, 番号（整数）の順にページに書き込む
		// MaxLength分に加えて、整数1個分の空き容量
		buf := make([]byte, npos+intByteSize)

		p := file.NewPageWithBuf(buf)
		require.NoError(t, p.SetString(0, s))
		require.NoError(t, p.SetInt(npos, 100+i))

		_, err := lm.Append(buf)
		require.NoError(t, err)
	}
}

func printLogRecords(t *testing.T, lm *LogManager) {
	t.Helper()
	li, err := lm.Iterator()
	require.NoError(t, err)

	fmt.Printf("\n======== printing... ========\n\n")

	for li.HasNext() {
		rec, err := li.Next()
		if err != nil {
			t.Fatalf("lm.Next() failed, %v", err)
		}

		page := file.NewPageWithBuf(rec)
		str, err := page.GetString(0)
		require.NoError(t, err)

		npos := file.MaxLengthInString(str)
		val, err := page.GetInt(npos)
		require.NoError(t, err)

		fmt.Printf("[%s , %d]\n", str, val)
		fmt.Printf("li.currentPos: %d, blocksize: %d, blk.Number(): %d\n",
			li.currentPos, li.fm.BlockSize(), li.blk.Number())
	}
}

func newLogManager(t *testing.T) *LogManager {
	fm := newFileManager(t)
	lm, err := NewLogManager(fm, "tempLogTest")

	require.NoError(t, err)

	return lm
}

func TestLogManager(t *testing.T) {
	lm := newLogManager(t)
	createRecords(t, lm, 1, 10)
	// record1  => [4byte(uint32) + page{[4byte(uint32) + 28byte(string)] + 4byte(uint32)}] => 40byte
	// record10 => [4byte(uint32) + page{[4byte(uint32) + 32byte(string)] + 4byte(uint32)}] => 44byte
	// 4 (boundary) + 40 * 9 => 4 + 360 = 364 で1回flush
	assert.Equal(t, 9, lm.lastSavedLSN)

	printLogRecords(t, lm)

	createRecords(t, lm, 11, 20)
	// 4 + 44 * 9 = 4 + 396 = 400 でflush
	assert.Equal(t, 18, lm.lastSavedLSN)

	err := lm.Flush(20)
	assert.NoError(t, err)

	printLogRecords(t, lm)

	// flushしたので、最後までディスクに書き込まれる。
	assert.Equal(t, 20, lm.lastSavedLSN)
}
