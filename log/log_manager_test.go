package log

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
		err := p.SetString(0, s)

		fmt.Printf("i: %d, buffer size %d, npos: %d, intByteSize %d\n", i, len(buf), npos, intByteSize)

		if err != nil {
			t.Fatalf("SetString failed, i = %d, err = %v", i, err)
		}

		err = p.SetInt(npos, 100+i)
		if err != nil {
			t.Fatalf("SetInt failed, i = %d, err = %v", i, err)
		}

		_, err = lm.Append(buf)
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

	fmt.Println("printing...")

	for li.HasNext() {
		rec, err := li.Next()
		if err != nil {
			t.Fatalf("lm.Next() failed, %v", err)
		}

		page := file.NewPageWithBuf(rec)
		str, err := page.GetString(0)

		if err != nil {
			t.Fatalf("page.GetString(0) failed, %v", err)
		}

		npos := file.MaxLength(str)
		val, err := page.GetInt(npos)

		if err != nil {
			t.Fatalf("page.GetInt(npos) failed, %v", err)
		}

		fmt.Printf("[%s , %d]", str, val)
	}
}

func newLogManager(t *testing.T) *LogManager {
	fm := newFileManaer(t)
	lm, err := NewLogManager(fm, "logtest")

	if err != nil {
		t.Fatalf("newLogManager() failed")
	}

	return lm
}

func TestLogManager(t *testing.T) {
	lm := newLogManager(t)
	createRecords(t, lm, 1, 35)
	// record1  => [8byte(int) + page{[8byte(int) + 7byte(string)] + 8byte(int)}] => 31byte
	// record10 => [8byte(int) + page{[8byte(int) + 8byte(string)] + 8byte(int)}] => 32byte
	// 8 + 31 * 10 + 32 * 2 => 8 + 310 + 64 = 372 でflush
	// 8 + 32 * 12 = 392でflush
	// => 35まではflushが入らないのでlastSavedLSNは24
	if lm.lastSavedLSN != 24 {
		t.Errorf("lastSavedLSN should be 24, but given %d", lm.lastSavedLSN)
	}

	// printLogRecords(t, lm)

	createRecords(t, lm, 36, 70)
	// flushは12の倍数で発生する。最後にflushするのは60
	if lm.lastSavedLSN != 60 {
		t.Errorf("lastSavedLSN should be 60, but given %d", lm.lastSavedLSN)
	}

	lm.Flush(65)

	// flushしたので、最後までディスクに書き込まれる。
	if lm.lastSavedLSN != 70 {
		t.Errorf("lastSavedLSN should be 60, but given %d", lm.lastSavedLSN)
	}
}
