package bytebuffer

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

// 整数を入れるときはuint32
const IntByteSize = 4

const CharsetMaxSize = utf8.UTFMax

type ByteBuffer struct {
	buf []byte // contents are the bytes buf[off : len(buf)]
	pos int    // read at &buf[off], write at &buf[len(buf)]
}

func New(bs int) *ByteBuffer {
	return &ByteBuffer{make([]byte, bs), 0}
}

func NewWithBuf(buf []byte) *ByteBuffer {
	return &ByteBuffer{buf, 0}
}

// SetPosition sets bb.pos
func (bb *ByteBuffer) SetPosition(pos int) error {
	if pos >= bb.Size() {
		return fmt.Errorf("bytebuffer: Position() cannot set position %d", pos)
	}
	bb.pos = pos
	return nil
}

// SetZeroPosition sets bb.pos=0
func (bb *ByteBuffer) SetZeroPosition() {
	bb.pos = 0
}

// CurrentPosition() returns current position
func (bb *ByteBuffer) CurrentPosition() int {
	return bb.pos
}

// Get copies bb.buf to buf and advance position the length of buf
func (bb *ByteBuffer) Get(buf []byte) error {
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
func (bb *ByteBuffer) GetInt() int {
	byteLen := readInt(bb.buf[bb.pos:])
	bb.pos += IntByteSize

	return int(byteLen)
}

// GetIntWithPosition get integers at the specified position (pos)
func (bb *ByteBuffer) GetIntWithPosition(pos int) (int, error) {
	if pos+IntByteSize > bb.Size() {
		return 0, fmt.Errorf("bytebuffer: GetIntWithPositoin() cannot get with position %d", pos)
	}

	bb.pos = pos
	bytelen := readInt(bb.buf[bb.pos:])

	bb.pos += IntByteSize
	return int(bytelen), nil
}

// PutInt set integer in current position and advance position the size of val
func (bb *ByteBuffer) PutInt(val int) error {
	if !bb.canStoreInt(val) {
		return fmt.Errorf("bytebuffer: PutInt() cannot put '%d'", val)
	}

	b := make([]byte, IntByteSize)
	putInt(b, val)

	copy(bb.buf[bb.pos:], b)
	bb.pos += IntByteSize
	return nil
}

// Put set []byte in current position and advance position the size of []byte
func (bb *ByteBuffer) Put(b []byte) error {
	if !bb.canStoreBytes(b) {
		return fmt.Errorf("bytebuffer: Put() cannot put []byte '%s'", b)
	}

	copy(bb.buf[bb.pos:], b)
	bb.pos += len(b)
	return nil
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
	return bb.pos + IntByteSize
}

func (bb *ByteBuffer) sizeNeedsToStoreBytes(b []byte) int {
	return bb.pos + len(b)
}

func readInt(buf []byte) uint32 {
	return endian().Uint32(buf)
}

func putInt(buf []byte, val int) {
	endian().PutUint32(buf, uint32(val))
}

func endian() binary.ByteOrder {
	return binary.LittleEndian
}

// 与えられた文字長の文字列がとりうる最大の容量を返す
func MaxLength(strlen int) int {
	return IntByteSize + strlen*CharsetMaxSize
}
