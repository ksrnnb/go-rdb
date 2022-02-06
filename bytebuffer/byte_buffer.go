package bytebuffer

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// 整数を入れるときはint64
const Int64Size = 64

type ByteBuffer struct {
	buf []byte // contents are the bytes buf[off : len(buf)]
	pos int    // read at &buf[off], write at &buf[len(buf)]
	err error
}

func NewWithBlockSize(bs int) *ByteBuffer {
	return &ByteBuffer{make([]byte, bs), 0, nil}
}

func New(buf []byte) *ByteBuffer {
	return &ByteBuffer{buf, 0, nil}
}

// Position sets bb.pos
func (bb *ByteBuffer) Position(pos int) {
	if pos >= bb.Size() {
		bb.err = fmt.Errorf("bytebuffer: Position() cannot set position %d", pos)
	} else {
		bb.pos = pos
	}
}

// Get copies bb.buf to buf and advance position the length of buf
func (bb *ByteBuffer) Get(buf []byte) error {
	if bb.err != nil {
		return bb.err
	}

	len := len(buf)
	tail := bb.pos + len

	if bb.sizeNeedsToStoreBytes(buf) >= bb.Size() {
		return fmt.Errorf("bytebuffer: Get() cannot get []byte")
	}

	copy(buf[0:], bb.buf[bb.pos:tail])
	bb.pos += len
	return nil
}

// GetInt gets integer in current posotion
func (bb *ByteBuffer) GetInt() (int, error) {
	if bb.err != nil {
		return 0, bb.err
	}

	b := bytes.NewBuffer(bb.buf[bb.pos:])
	byteLen, err := binary.ReadVarint(b)

	if err != nil {
		return 0, nil
	}

	s := intSize(byteLen)
	bb.pos += s

	return int(byteLen), err
}

// GetIntWithPosition get integers at the specified position (pos)
func (bb *ByteBuffer) GetIntWithPosition(pos int) (int, error) {
	if pos+Int64Size > bb.Size() {
		return 0, fmt.Errorf("bytebuffer: GetIntWithPositoin() cannot get with position %d", pos)
	}

	bb.pos = pos
	b := bytes.NewBuffer(bb.buf[bb.pos:])
	bytelen, err := binary.ReadVarint(b)

	bb.pos += intSize(bytelen)
	return int(bytelen), err
}

// PutInt set integer in current position and advance position the size of val
func (bb *ByteBuffer) PutInt(val int) {
	if bb.err != nil {
		return
	}

	if bb.sizeNeedsToStoreInt() >= bb.Size() {
		bb.err = fmt.Errorf("bytebuffer: PutInt() cannot put '%d'", val)
		return
	}

	size := binary.PutVarint(bb.buf, int64(val))
	bb.pos += size
}

// Put set []byte in current position and advance position the size of []byte
func (bb *ByteBuffer) Put(b []byte) {
	if bb.err != nil {
		return
	}

	if bb.sizeNeedsToStoreBytes(b) >= bb.Size() {
		bb.err = fmt.Errorf("bytebuffer: Put() cannot put []byte '%s'", b)
		return
	}

	copy(bb.buf[bb.pos:], b)
	bb.pos += len(b)
}

// binaryのPutUVarintを参照
func intSize(x int64) int {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}

func (bb *ByteBuffer) Error() error {
	return bb.err
}

func (bb *ByteBuffer) Size() int {
	return len(bb.buf)
}

func (bb *ByteBuffer) sizeNeedsToStoreInt() int {
	return bb.pos + Int64Size
}

func (bb *ByteBuffer) sizeNeedsToStoreBytes(b []byte) int {
	return bb.pos + Int64Size + len(b)
}
