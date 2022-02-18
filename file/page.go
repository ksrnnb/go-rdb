package file

import (
	"github.com/ksrnnb/go-rdb/bytebuffer"
)

type Page struct {
	bb  *bytebuffer.ByteBuffer
	err error
}

func NewPage(bs int) *Page {
	bb := bytebuffer.New(bs)
	return &Page{bb, nil}
}

func NewPageWithBuf(buf []byte) *Page {
	bb := bytebuffer.NewWithBuf(buf)

	return &Page{bb, nil}
}

// 指定した位置の文字列を取得して返す
func (p *Page) GetBytes(pos int) []byte {
	if p.err != nil {
		return []byte{}
	}
	bytelen := p.GetInt(pos)
	newByte := make([]byte, bytelen)
	p.bb.Get(newByte)

	if err := p.bb.Err(); err != nil {
		p.err = err
		return []byte{}
	}

	return newByte
}

// SetBytes()はページの指定した位置に2つの値を格納する
// 1個目はバイトの長さ、つづいてバイト列を格納する。
func (p *Page) SetBytes(pos int, b []byte) {
	if p.err != nil {
		return
	}

	p.bb.Position(pos)
	p.bb.PutInt(len(b))
	p.bb.Put(b)

	if err := p.bb.Err(); err != nil {
		p.err = err
		return
	}
}

// 指定した位置の文字列を取得する
func (p *Page) GetString(pos int) string {
	if p.err != nil {
		return ""
	}

	b := p.GetBytes(pos)
	return string(b)
}

// 指定した位置に文字列を格納
func (p *Page) SetString(pos int, s string) {
	if p.err != nil {
		return
	}

	b := []byte(s)
	p.SetBytes(pos, b)
}

// 指定した位置の整数を取得
func (p *Page) GetInt(pos int) int {
	if p.err != nil {
		return 0
	}

	return p.bb.GetIntWithPosition(pos)
}

// 指定した位置の整数を返す
func (p *Page) SetInt(pos, val int) {
	if p.err != nil {
		return
	}
	p.bb.Position(pos)
	p.bb.PutInt(val)

	if err := p.bb.Err(); err != nil {
		p.err = err
	}
}

// ページのバッファを全て読み込んで返す
func (p *Page) ReadBuf() []byte {
	return p.bb.ReadBuf()
}

// 指定したバイト配列をページの先頭から書き込む
func (p *Page) WriteBuf(b []byte) {
	if p.err != nil {
		return
	}

	p.bb.Position(0)
	p.bb.WriteBuf(b)

	if err := p.bb.Err(); err != nil {
		p.err = err
		return
	}
}

// 位置を先頭に戻してバッファを返す
func (p *Page) Contents() *bytebuffer.ByteBuffer {
	p.bb.Position(0)
	return p.bb
}

// Position() returns current bytebuffer position
func (p *Page) Position() int {
	return p.bb.CurrentPosition()
}

// エラーを返す
func (p *Page) Err() error {
	return p.err
}

// 文字列の長さから、バッファに格納するのに必要なサイズを返す
func MaxLength(str string) int {
	return bytebuffer.MaxLength(str)
}
