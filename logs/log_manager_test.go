package logs

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/ksrnnb/go-rdb/file"
)

func createRecords(t *testing.T, lm *LogManager, start, end int) {
	t.Helper()

	for i := start; i <= end; i++ {
		s := "record" + strconv.Itoa(i)
		npos := file.MaxLength(s)

		// 文字列長（整数）, 文字列, 番号（整数）の順にページに書き込む
		// MaxLength分に加えて、整数1個分の空き容量
		buf := make([]byte, npos+intByteSize)

		p := file.NewPageWithBuf(buf)
		p.SetString(0, s)
		p.SetInt(npos, 100+i)
		if err := p.Err(); err != nil {
			t.Fatalf("SetInt failed, i = %d, err = %v", i, err)
		}

		_, err := lm.Append(buf)
		if err != nil {
			t.Fatalf("Append failed, i = %d, err = %v", i, err)
		}
	}
}

func printLogRecords(t *testing.T, lm *LogManager) {
	t.Helper()
	li, err := lm.Iterator()
	if err != nil {
		t.Fatalf("lm.Iterator() failed, %v", err)
	}

	fmt.Printf("\n======== printing... ========\n\n")

	for li.HasNext() {
		rec, err := li.Next()
		if err != nil {
			t.Fatalf("lm.Next() failed, %v", err)
		}

		page := file.NewPageWithBuf(rec)
		str := page.GetString(0)
		npos := file.MaxLength(str)
		val := page.GetInt(npos)

		if err := page.Err(); err != nil {
			t.Fatalf("page.GetInt(npos) failed, %v", err)
		}

		fmt.Printf("[%s , %d]\n", str, val)
		fmt.Printf("li.currentPos: %d, blocksize: %d, blk.Number(): %d\n",
			li.currentPos, li.fm.BlockSize(), li.blk.Number())
	}
}

func newLogManager(t *testing.T) *LogManager {
	fm := newFileManager(t)
	lm, err := NewLogManager(fm, "tempLogTest")

	if err != nil {
		t.Fatalf("newLogManager() failed")
	}

	return lm
}

func TestLogManager(t *testing.T) {
	lm := newLogManager(t)
	createRecords(t, lm, 1, 35)
	// record1  => [4byte(uint32) + page{[4byte(uint32) + 7byte(string)] + 4byte(uint32)}] => 19byte
	// record10 => [4byte(uint32) + page{[4byte(uint32) + 8byte(string)] + 4byte(uint32)}] => 20byte
	// 8 + 19 * 9 + 20 * 11 => 8 + 171 + 220 = 399 でflush
	// => 35まではflushが入らないのでlastSavedLSNは24
	if lm.lastSavedLSN != 20 {
		t.Errorf("lastSavedLSN should be 20, but given %d", lm.lastSavedLSN)
	}

	printLogRecords(t, lm)

	createRecords(t, lm, 36, 70)
	// 8 + 20 * 19 = 388でflush
	// flushは20 + 19 * nで発生する。最後にflushするのは58
	if lm.lastSavedLSN != 58 {
		t.Errorf("lastSavedLSN should be 58, but given %d", lm.lastSavedLSN)
	}

	lm.Flush(65)

	printLogRecords(t, lm)

	// flushしたので、最後までディスクに書き込まれる。
	if lm.lastSavedLSN != 70 {
		t.Errorf("lastSavedLSN should be 70, but given %d", lm.lastSavedLSN)
	}
}
