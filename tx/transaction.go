package tx

import "github.com/ksrnnb/go-rdb/file"

type Transaction struct {
	nextTxNum int
}

func (tx *Transaction) Pin(blk *file.BlockID) {

}

func (tx *Transaction) Unpin(blk *file.BlockID) {

}

func (tx *Transaction) SetString(blk *file.BlockID, offset int, val string, okToLog bool) {

}

func (tx *Transaction) SetInt(blk *file.BlockID, offset int, val int, okToLog bool) {

}
