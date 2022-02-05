package file

import (
	"fmt"

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
	// TODO: implement
	bb := bytebuffer.New(buf)

	return &Page{bb}
}

func (p *Page) GetBytes(pos int) ([]byte, error) {
	p.bb.Position(pos)
	bytelen, err := p.bb.GetInt()

	if err != nil {
		// TODO: wrap error
		return []byte{}, err
	}

	newByte := make([]byte, bytelen)
	p.bb.Get(newByte)

	fmt.Println("newByte", string(newByte))
	return newByte, nil
}

func (p *Page) SetBytes(pos int, b []byte) {
	p.bb.Position(pos)
	p.bb.PutInt(len(b))
	p.bb.Put(b)
}

func (p *Page) GetInt(pos int) (int, error) {
	return p.bb.GetIntWithPosition(pos)
}
