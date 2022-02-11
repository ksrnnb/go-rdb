package bytebuffer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

// 整数を入れるときはint64
const IntByteSize = 8

type ByteBuffer struct {
	buf []byte // contents are the bytes buf[off : len(buf)]
	pos int    // read at &buf[off], write at &buf[len(buf)]
	err error
}

func New(bs int) *ByteBuffer {
	return &ByteBuffer{make([]byte, bs), 0, nil}
}

func NewWithBuf(buf []byte) *ByteBuffer {
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

// CurrentPosition() returns current position
func (bb *ByteBuffer) CurrentPosition() int {
	return bb.pos
}

// Get copies bb.buf to buf and advance position the length of buf
func (bb *ByteBuffer) Get(buf []byte) error {
	if bb.err != nil {
		return bb.err
	}

	len := len(buf)
	tail := bb.pos + len

	if !bb.canStoreBytes(buf) {
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
	if pos+IntByteSize > bb.Size() {
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

	if !bb.canStoreInt(val) {
		bb.err = fmt.Errorf("bytebuffer: PutInt() cannot put '%d'", val)
		return
	}

	b := make([]byte, IntByteSize)
	n := binary.PutVarint(b, int64(val))

	copy(bb.buf[bb.pos:], b[:n])
	bb.pos += n
}

// Put set []byte in current position and advance position the size of []byte
func (bb *ByteBuffer) Put(b []byte) {
	if bb.err != nil {
		return
	}

	if !bb.canStoreBytes(b) {
		bb.err = fmt.Errorf("bytebuffer: Put() cannot put []byte '%s'", b)
		return
	}

	copy(bb.buf[bb.pos:], b)
	bb.pos += len(b)
}

func (bb *ByteBuffer) ReadBuf() []byte {
	return bb.buf
}

func (bb *ByteBuffer) WriteBuf(b []byte) error {
	if len(b) > len(bb.buf) {
		return fmt.Errorf("bytebuffer: WriteBuf(b) failed, %s", b)
	}

	head := bb.pos
	tail := bb.pos + len(b)

	if tail > head {
		copy(bb.buf[head:tail], b)
	}

	return nil
}

func (bb *ByteBuffer) Error() error {
	return bb.err
}

func (bb *ByteBuffer) Size() int {
	return len(bb.buf)
}

func (bb *ByteBuffer) canStoreInt(val int) bool {
	return bb.Size() >= bb.sizeNeedsToStoreInt(val)
}

func (bb *ByteBuffer) canStoreBytes(buf []byte) bool {
	return bb.Size() >= bb.sizeNeedsToStoreBytes(buf)
}

func (bb *ByteBuffer) sizeNeedsToStoreInt(val int) int {
	return bb.pos + intSize(int64(val))
}

func (bb *ByteBuffer) sizeNeedsToStoreBytes(b []byte) int {
	return bb.pos + len(b)
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

// 与えられた文字長の文字列がとりうる最大の容量を返す
func MaxLength(str string) int {
	return IntByteSize + len(str)
	// Goのlenは引数が何バイトか返してくれるので、len(str) * maxBytesPerChar()とするのは誤り
	// return IntByteSize + strlen*maxBytesPerChar()
}

// 文字列から文字長のバイト数を返す
func GetByteLength(str []byte) int {
	l := len(str)
	return intSize(int64(l))
}

// UTF-8を想定
func maxBytesPerChar() int {
	return utf8.UTFMax
}
