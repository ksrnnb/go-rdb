package file

import (
	"github.com/ksrnnb/go-rdb/bytebuffer"
)

type Page struct {
	bb *bytebuffer.ByteBuffer
}

func New(bs int) *Page {
	bb := bytebuffer.New(bs)
	return &Page{bb}
}

func NewBuf(buf []byte) *Page {
	bb := bytebuffer.NewBuf(buf)

	return &Page{bb}
}

func (p *Page) GetBytes(pos int) ([]byte, error) {
	bytelen, err := p.GetInt(pos)

	if err != nil {
		// TODO: wrap error
		return []byte{}, err
	}

	newByte := make([]byte, bytelen)
	err = p.bb.Get(newByte)
	return newByte, err
}

func (p *Page) SetBytes(pos int, b []byte) error {
	p.bb.Position(pos)
	p.bb.PutInt(len(b))
	p.bb.Put(b)

	return p.bb.Error()
}

func (p *Page) GetInt(pos int) (int, error) {
	return p.bb.GetIntWithPosition(pos)
}

func (p *Page) SetInt(pos, val int) error {
	p.bb.Position(pos)
	p.bb.PutInt(val)

	return p.bb.Error()
}

func (p *Page) Size() int {
	return p.bb.Size()
}
