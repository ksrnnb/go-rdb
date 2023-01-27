package btree

import (
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type BTreeLeaf struct {
	tx          *tx.Transaction
	layout      *record.Layout
	searchKey   query.Constant
	contents    *BTreePage
	currentSlot int
	fileName    string
}

func NewBTreeLeaf(tx *tx.Transaction, blk *file.BlockID, layout *record.Layout, searchKey query.Constant) (*BTreeLeaf, error) {
	btp, err := NewBTreePage(tx, blk, layout)
	if err != nil {
		return nil, err
	}
	currentSlot, err := btp.FindSlotBefore(searchKey)
	if err != nil {
		return nil, err
	}

	btl := &BTreeLeaf{
		tx:          tx,
		layout:      layout,
		searchKey:   searchKey,
		contents:    btp,
		currentSlot: currentSlot,
		fileName:    blk.FileName(),
	}
	return btl, nil
}

func (btl *BTreeLeaf) Close() error {
	return btl.contents.Close()
}

func (btl *BTreeLeaf) HasNext() (bool, error) {
	numRecords, err := btl.contents.getNumRecords()
	if err != nil {
		return false, err
	}
	btl.currentSlot++
	if btl.currentSlot >= numRecords {
		return btl.tryOverflow()
	}
	val, err := btl.contents.GetDataValue(btl.currentSlot)
	if err != nil {
		return false, err
	}
	if val.Equals(btl.searchKey) {
		return true, nil
	}
	return btl.tryOverflow()
}

func (btl *BTreeLeaf) GetDataRid() (*record.RecordID, error) {
	return btl.contents.getDataRid(btl.currentSlot)
}

func (btl *BTreeLeaf) Delete(target *record.RecordID) error {
	hasNext, err := btl.HasNext()
	if err != nil {
		return err
	}
	for hasNext {
		rid, err := btl.GetDataRid()
		if err != nil {
			return err
		}
		if rid.Equals(target) {
			btl.contents.delete(btl.currentSlot)
			return nil
		}
	}
	return nil
}

func (btl *BTreeLeaf) Insert(rid *record.RecordID) (DirectoryEntry, error) {
	f, err := btl.contents.GetFlag()
	if err != nil {
		return DirectoryEntry{}, err
	}
	firstVal, err := btl.contents.GetDataValue(0)
	if err != nil {
		return DirectoryEntry{}, err
	}
	// TODO: フラグの意味は？？
	if f >= 0 && firstVal.IsGreaterThan(btl.searchKey) {
		newBlk, err := btl.contents.Split(0, f)
		if err != nil {
			return DirectoryEntry{}, err
		}

		btl.currentSlot = 0
		err = btl.contents.SetFlag(-1)
		if err != nil {
			return DirectoryEntry{}, err
		}

		err = btl.contents.insertLeaf(btl.currentSlot, btl.searchKey, rid)
		if err != nil {
			return DirectoryEntry{}, err
		}
		return NewDirectoryEntry(firstVal, newBlk.Number()), nil
	}

	btl.currentSlot++
	err = btl.contents.insertLeaf(btl.currentSlot, btl.searchKey, rid)
	if err != nil {
		return DirectoryEntry{}, err
	}
	isFull, err := btl.contents.IsFull()
	if err != nil {
		return DirectoryEntry{}, err
	}
	if !isFull {
		return DirectoryEntry{}, nil
	}

	// page is full, so split it
	firstKey, err := btl.contents.GetDataValue(btl.currentSlot)
	if err != nil {
		return DirectoryEntry{}, err
	}
	numRecords, err := btl.contents.getNumRecords()
	if err != nil {
		return DirectoryEntry{}, err
	}
	lastKey, err := btl.contents.GetDataValue(numRecords - 1)
	if err != nil {
		return DirectoryEntry{}, err
	}
	if lastKey.Equals(firstKey) {
		// create overflow block to hold all but the first record
		f, err := btl.contents.GetFlag()
		if err != nil {
			return DirectoryEntry{}, err
		}
		newBlk, err := btl.contents.Split(1, f)
		if err != nil {
			return DirectoryEntry{}, err
		}
		err = btl.contents.SetFlag(newBlk.Number())
		if err != nil {
			return DirectoryEntry{}, err
		}
		return DirectoryEntry{}, nil
	}

	numRecords, err = btl.contents.getNumRecords()
	if err != nil {
		return DirectoryEntry{}, err
	}
	splitPos := numRecords / 2
	splitKey, err := btl.contents.GetDataValue(splitPos)
	if err != nil {
		return DirectoryEntry{}, err
	}
	if splitKey.Equals(firstKey) {
		// move right, looking for the next key
		v, err := btl.contents.GetDataValue(splitPos)
		if err != nil {
			return DirectoryEntry{}, err
		}
		for v.Equals(splitKey) {
			splitPos++
			nextV, err := btl.contents.GetDataValue(splitPos)
			if err != nil {
				return DirectoryEntry{}, err
			}
			v = nextV
		}
	} else {
		// move left, looking for the next key
		v, err := btl.contents.GetDataValue(splitPos - 1)
		if err != nil {
			return DirectoryEntry{}, err
		}
		for v.Equals(splitKey) {
			splitPos--
			nextV, err := btl.contents.GetDataValue(splitPos)
			if err != nil {
				return DirectoryEntry{}, err
			}
			v = nextV
		}
	}
	newBlk, err := btl.contents.Split(splitPos, -1)
	if err != nil {
		return DirectoryEntry{}, err
	}
	return NewDirectoryEntry(splitKey, newBlk.Number()), nil
}

func (btl *BTreeLeaf) tryOverflow() (bool, error) {
	firstKey, err := btl.contents.GetDataValue(0)
	if err != nil {
		return false, err
	}
	flag, err := btl.contents.GetFlag()
	if err != nil {
		return false, err
	}
	if !btl.searchKey.Equals(firstKey) || flag < 0 {
		return false, nil
	}
	if err := btl.contents.Close(); err != nil {
		return false, err
	}
	nextBlk := file.NewBlockID(btl.fileName, flag)
	newBTreePage, err := NewBTreePage(btl.tx, nextBlk, btl.layout)
	if err != nil {
		return false, err
	}
	btl.contents = newBTreePage
	btl.currentSlot = 0
	return true, nil
}
