package file

import (
	"github.com/ksrnnb/go-rdb/bytebuffer"
)

type Page struct {
	bb *bytebuffer.ByteBuffer
}

func NewWithBlockSize(bs int) *Page {
	bb := bytebuffer.NewWithBlockSize(bs)
	return &Page{bb}
}

func New(buf []byte) *Page {
	bb := bytebuffer.New(buf)

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

func (p *Page) Size() int {
	return p.bb.Size()
}
