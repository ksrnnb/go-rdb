package file

import (
	"unicode/utf8"

	"github.com/ksrnnb/go-rdb/bytebuffer"
)

type Page struct {
	bb *bytebuffer.ByteBuffer
}

func NewPage(bs int) *Page {
	bb := bytebuffer.New(bs)
	return &Page{bb}
}

func NewPageWithBuf(buf []byte) *Page {
	bb := bytebuffer.NewWithBuf(buf)

	return &Page{bb}
}

// 指定した位置の文字列を取得して返す
func (p *Page) GetBytes(pos int) ([]byte, error) {
	bytelen, err := p.GetInt(pos)
	if err != nil {
		return []byte{}, err
	}

	newByte := make([]byte, bytelen)
	if err := p.bb.Get(newByte); err != nil {
		return []byte{}, err
	}

	return newByte, nil
}

// SetBytes()はページの指定した位置に2つの値を格納する
// 1個目はバイトの長さ、つづいてバイト列を格納する。
func (p *Page) SetBytes(pos int, b []byte) error {
	if err := p.bb.SetPosition(pos); err != nil {
		return err
	}

	if err := p.bb.PutInt(len(b)); err != nil {
		return err
	}

	return p.bb.Put(b)
}

// 指定した位置の文字列を取得する
func (p *Page) GetString(pos int) (string, error) {
	b, err := p.GetBytes(pos)
	return string(b), err
}

// 指定した位置に文字列を格納
func (p *Page) SetString(pos int, s string) error {
	b := []byte(s)
	return p.SetBytes(pos, b)
}

// 指定した位置の整数を取得
func (p *Page) GetInt(pos int) (int, error) {
	return p.bb.GetIntWithPosition(pos)
}

// 指定した位置の整数を返す
func (p *Page) SetInt(pos, val int) error {

	if err := p.bb.SetPosition(pos); err != nil {
		return err
	}

	return p.bb.PutInt(val)
}

// ページのバッファを全て読み込んで返す
func (p *Page) ReadBuf() []byte {
	return p.bb.ReadBuf()
}

// 指定したバイト配列をページの先頭から書き込む
func (p *Page) WriteBuf(b []byte) error {
	if err := p.bb.SetPosition(0); err != nil {
		return err
	}
	return p.bb.WriteBuf(b)
}

// 位置を先頭に戻してバッファを返す
func (p *Page) Contents() *bytebuffer.ByteBuffer {
	p.bb.SetZeroPosition()
	return p.bb
}

// Position() returns current bytebuffer position
func (p *Page) Position() int {
	return p.bb.CurrentPosition()
}

// 文字列の長さから、バッファに格納するのに必要なサイズを返す
func MaxLength(strlen int) int {
	return bytebuffer.MaxLength(strlen)
}

// 文字列から、バッファに格納するのに必要なサイズを返す
func MaxLengthInString(str string) int {
	return MaxLength(utf8.RuneCountInString(str))
}
