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

// NewBTreeLeaf は特定のブロックの B-tree page を作成し、searchKey の前のレコード位置に移動する
func NewBTreeLeaf(tx *tx.Transaction, blk file.BlockID, layout *record.Layout, searchKey query.Constant) (*BTreeLeaf, error) {
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

// HasNext は currentSlot を次に進めて、同じ値をもつかどうかを返す
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

// Insert は繰り返し HasNext を呼び、指定のレコード ID を探して削除する
// 実行される前に BeforeFirst が呼ばれていると仮定している
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

// Insert は次のレコードに移動し、引数で渡したレコードを挿入する
// 実行される前に BeforeFirst が呼ばれていると仮定している
func (btl *BTreeLeaf) Insert(rid *record.RecordID) (DirectoryEntry, error) {
	f, err := btl.contents.GetFlag()
	if err != nil {
		return DirectoryEntry{}, err
	}
	firstVal, err := btl.contents.GetDataValue(0)
	if err != nil {
		return DirectoryEntry{}, err
	}
	// いまみている B-Tree Leaf が overflow しているときは、slot = 0 のみレコードが存在する
	// slot = 0 のレコードは一番小さい値にしたいので、searchKey の方が小さい場合は処理が必要
	// 本でいうと、Fig. 12.20
	if f.HasOverflow() && firstVal.IsGreaterThan(btl.searchKey) {
		// overflow しているデータは分割して新しい B-Tree page に移す
		newBlk, err := btl.contents.Split(0, f)
		if err != nil {
			return DirectoryEntry{}, err
		}

		// currentSlot には insert したいレコードを入れる
		btl.currentSlot = 0
		err = btl.contents.SetFlag(NoOverFlow)
		if err != nil {
			return DirectoryEntry{}, err
		}
		err = btl.contents.insertLeaf(btl.currentSlot, btl.searchKey, rid)
		if err != nil {
			return DirectoryEntry{}, err
		}
		// 新しく追加したブロックのディレクトリエントリを返す
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
		// full でなければ新しくブロックを追加してないので空のディレクトリエントリを返す
		return DirectoryEntry{}, nil
	}

	// page is full, so split it
	firstKey, err := btl.contents.GetDataValue(0)
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

	// 全部同じレコード => overflow
	if lastKey.Equals(firstKey) {
		// create overflow block to hold all but the first record
		f, err := btl.contents.GetFlag()
		if err != nil {
			return DirectoryEntry{}, err
		}
		// 先頭のレコードは残す
		newBlk, err := btl.contents.Split(1, f)
		if err != nil {
			return DirectoryEntry{}, err
		}
		// overflow 先のブロック番号をフラグに設定する
		err = btl.contents.SetFlag(PageFlag(newBlk.Number()))
		if err != nil {
			return DirectoryEntry{}, err
		}
		// overflow ブロックを作成した場合はディレクトリエントリは空を返す
		return DirectoryEntry{}, nil
	}

	// ざっくり二分したあとは線形に探索している
	splitPos := numRecords / 2
	splitKey, err := btl.contents.GetDataValue(splitPos)
	if err != nil {
		return DirectoryEntry{}, err
	}
	if splitKey.Equals(firstKey) {
		// move right, looking for the next key
		v := splitKey
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
	newBlk, err := btl.contents.Split(splitPos, NoOverFlow)
	if err != nil {
		return DirectoryEntry{}, err
	}
	return NewDirectoryEntry(splitKey, newBlk.Number()), nil
}

// tryOverflow は overflow chain を含む leaf block を扱う
// overflow がない場合は false, ある場合は B-Tree page と currentSlot を更新して true を返す
func (btl *BTreeLeaf) tryOverflow() (bool, error) {
	firstKey, err := btl.contents.GetDataValue(0)
	if err != nil {
		return false, err
	}
	flag, err := btl.contents.GetFlag()
	if err != nil {
		return false, err
	}
	if !(btl.searchKey.Equals(firstKey) && flag.HasOverflow()) {
		return false, nil
	}
	if err := btl.contents.Close(); err != nil {
		return false, err
	}
	nextBlk := file.NewBlockID(btl.fileName, flag.AsInt())
	newBTreePage, err := NewBTreePage(btl.tx, nextBlk, btl.layout)
	if err != nil {
		return false, err
	}
	btl.contents = newBTreePage
	btl.currentSlot = 0
	return true, nil
}
