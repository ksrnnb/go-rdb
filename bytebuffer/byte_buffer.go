package bytebuffer

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var ErrTooLarge = errors.New("bytebuffer: too large")

const maxInt = int(^uint(0) >> 1)

// smallBufferSize is an initial allocation minimal capacity.
const smallBufferSize = 64

type ByteBuffer struct {
	buf []byte // contents are the bytes buf[off : len(buf)]
	pos int    // read at &buf[off], write at &buf[len(buf)]
}

func NewWithBlockSize(bs int) *ByteBuffer {
	return &ByteBuffer{make([]byte, bs), 0}
}

func New(buf []byte) *ByteBuffer {
	return &ByteBuffer{buf, 0}
}

// Position sets bb.pos
func (bb *ByteBuffer) Position(pos int) {
	bb.pos = pos
}

// Get copies bb.buf to buf and advance position the length of buf
func (bb *ByteBuffer) Get(buf []byte) {
	len := len(buf)
	tail := bb.pos + len
	copy(buf[0:], bb.buf[bb.pos:tail])

	bb.pos += len
}

// GetInt gets integer in current posotion
func (bb *ByteBuffer) GetInt() (int, error) {
	b := bytes.NewBuffer(bb.buf[bb.pos:])
	byteLen, err := binary.ReadVarint(b)

	s := bb.intSize(byteLen)
	bb.pos += s

	return int(byteLen), err
}

// GetIntWithPosition get integers at the specified position (pos)
func (bb *ByteBuffer) GetIntWithPosition(pos int) (int, error) {
	bb.pos = pos
	b := bytes.NewBuffer(bb.buf[bb.pos:])
	bytelen, err := binary.ReadVarint(b)

	return int(bytelen), err
}

// PutInt set integer in current position and advance position the size of val
func (bb *ByteBuffer) PutInt(val int) {
	ok := bb.tryGrowByReslice(val)
	if !ok {
		bb.grow(val)
	}

	size := binary.PutVarint(bb.buf, int64(val))
	bb.pos += size
}

// Put set []byte in current position and advance position the size of []byte
func (bb *ByteBuffer) Put(b []byte) {
	ok := bb.tryGrowByReslice(len(b))
	if !ok {
		bb.grow(len(b))
	}
	copy(bb.buf[bb.pos:], b)

	bb.pos += len(b)
}

// binaryのPutUVarintを参照
func (bb *ByteBuffer) intSize(x int64) int {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}

// ここから下はbytes.Bufferを参照
// Reset resets the buffer to be empty,
// but it retains the underlying storage for use by future writes.
// Reset is the same as Truncate(0).
func (bb *ByteBuffer) Reset() {
	bb.buf = bb.buf[:0]
	bb.pos = 0
}

// tryGrowByReslice is a inlineable version of grow for the fast-case where the
// internal buffer only needs to be resliced.
// It returns the index where bytes should be written and whether it succeeded.
func (bb *ByteBuffer) tryGrowByReslice(n int) bool {
	if l := len(bb.buf); n <= cap(bb.buf)-l {
		bb.buf = bb.buf[:l+n]
		return true
	}
	return false
}

// grow grows the buffer to guarantee space for n more bytes.
// It returns the index where bytes should be written.
// If the buffer can't grow it will panic with ErrTooLarge.
func (bb *ByteBuffer) grow(n int) {
	m := len(bb.buf)
	// If buffer is empty, reset to recover space.
	if m == 0 && bb.pos != 0 {
		bb.Reset()
	}
	// Try to grow by means of a reslice.
	if ok := bb.tryGrowByReslice(n); ok {
		return
	}
	if bb.buf == nil && n <= smallBufferSize {
		bb.buf = make([]byte, n, smallBufferSize)
		return
	}
	c := cap(bb.buf)
	if n <= c/2-m {
		// We can slide things down instead of allocating a new
		// slice. We only need m+n <= c to slide, but
		// we instead let capacity get twice as large so we
		// don't spend all our time copying.
		copy(bb.buf, bb.buf[bb.pos:])
	} else if c > maxInt-c-n {
		panic(ErrTooLarge)
	} else {
		// Not enough space anywhere, we need to allocate.
		buf := makeSlice(2*c + n)
		copy(buf, bb.buf[bb.pos:])
		bb.buf = buf
	}
	// Restore b.pos and len(b.buf).
	bb.pos = 0
	bb.buf = bb.buf[:m+n]
}

// makeSlice allocates a slice of size n. If the allocation fails, it panics
// with ErrTooLarge.
func makeSlice(n int) []byte {
	// If the make fails, give a known error.
	defer func() {
		if recover() != nil {
			panic(ErrTooLarge)
		}
	}()
	return make([]byte, n)
}
