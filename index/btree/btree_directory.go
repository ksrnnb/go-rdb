package btree

import (
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type BTreeDirectory struct {
	tx       *tx.Transaction
	layout   *record.Layout
	contents *BTreePage
	fileName string
}

func NewBTreeDirectory(tx *tx.Transaction, blk file.BlockID, layout *record.Layout) (*BTreeDirectory, error) {
	btp, err := NewBTreePage(tx, blk, layout)
	if err != nil {
		return nil, err
	}
	return &BTreeDirectory{
		tx:       tx,
		layout:   layout,
		contents: btp,
		fileName: blk.FileName(),
	}, nil
}

func (btd *BTreeDirectory) Close() error {
	return btd.contents.Close()
}

// Search はルートから開始して tree を level-0 のディレクトリのブロックまで下る
// searchKey を含む leaf のブロック番号を返す
func (btd *BTreeDirectory) Search(searchKey query.Constant) (int, error) {
	childBlk, err := btd.findChildBlock(searchKey)
	if err != nil {
		return 0, err
	}
	level, err := btd.contents.GetFlag()
	if err != nil {
		return 0, err
	}
	for level > 0 {
		err := btd.contents.Close()
		if err != nil {
			return 0, err
		}
		btp, err := NewBTreePage(btd.tx, childBlk, btd.layout)
		if err != nil {
			return 0, err
		}
		btd.contents = btp
		childBlk, err = btd.findChildBlock(searchKey)
		if err != nil {
			return 0, err
		}
		newLevel, err := btd.contents.GetFlag()
		if err != nil {
			return 0, err
		}
		level = newLevel
	}

	return childBlk.Number(), nil
}

// MakeNewRoot は root page への insert が non null だった場合に呼ばれる
func (btd *BTreeDirectory) MakeNewRoot(de DirectoryEntry) error {
	firstVal, err := btd.contents.GetDataValue(0)
	if err != nil {
		return err
	}
	level, err := btd.contents.GetFlag()
	if err != nil {
		return err
	}
	newBlk, err := btd.contents.Split(0, level)
	if err != nil {
		return err
	}
	oldRoot := NewDirectoryEntry(firstVal, newBlk.Number())
	_, err = btd.insertEntry(oldRoot)
	if err != nil {
		return err
	}
	_, err = btd.insertEntry(de)
	if err != nil {
		return err
	}
	return btd.contents.SetFlag(level + 1)
}

// Insert はルートから開始して再帰的に tree を level-0 のディレクトリのブロックまで下って
// ディレクトリエントリを追加する
func (btd *BTreeDirectory) Insert(de DirectoryEntry) (DirectoryEntry, error) {
	level, err := btd.contents.GetFlag()
	if err != nil {
		return emptyDir, err
	}
	if level == 0 {
		return btd.insertEntry(de)
	}
	childBlk, err := btd.findChildBlock(de.DataValue())
	if err != nil {
		return emptyDir, err
	}
	child, err := NewBTreeDirectory(btd.tx, childBlk, btd.layout)
	if err != nil {
		return emptyDir, err
	}
	myEntry, err := child.Insert(de)
	if err != nil {
		return emptyDir, err
	}
	if err := child.Close(); err != nil {
		return emptyDir, err
	}
	if myEntry.IsZero() {
		return emptyDir, nil
	}
	return btd.insertEntry(myEntry)
}

// insertEntry はページにディレクトリエントリを挿入する
// 分割が発生した場合は新しいディレクトリエントリを返す
func (btd *BTreeDirectory) insertEntry(de DirectoryEntry) (DirectoryEntry, error) {
	n, err := btd.contents.FindSlotBefore(de.DataValue())
	if err != nil {
		return emptyDir, err
	}
	newSlot := n + 1
	err = btd.contents.insertDirectory(newSlot, de.DataValue(), de.BlockNumber())
	if err != nil {
		return emptyDir, err
	}

	isFull, err := btd.contents.IsFull()
	if err != nil {
		return emptyDir, err
	}
	if !isFull {
		return emptyDir, nil
	}

	// full になったら分割する
	level, err := btd.contents.GetFlag()
	if err != nil {
		return emptyDir, err
	}
	splitPos, err := btd.contents.getNumRecords()
	if err != nil {
		return emptyDir, err
	}
	splitVal, err := btd.contents.GetDataValue(splitPos)
	if err != nil {
		return emptyDir, err
	}
	newBlk, err := btd.contents.Split(splitPos, level)
	if err != nil {
		return emptyDir, err
	}
	return NewDirectoryEntry(splitVal, newBlk.Number()), nil
}

func (btd *BTreeDirectory) findChildBlock(searchKey query.Constant) (file.BlockID, error) {
	slot, err := btd.contents.FindSlotBefore(searchKey)
	if err != nil {
		return file.BlockID{}, err
	}
	v, err := btd.contents.GetDataValue(slot + 1)
	if err != nil {
		return file.BlockID{}, err
	}
	if v.Equals(searchKey) {
		slot++
	}
	blkNum, err := btd.contents.getChildNum(slot)
	if err != nil {
		return file.BlockID{}, err
	}
	return file.NewBlockID(btd.fileName, blkNum), nil
}
