package btree

import (
	"fmt"
	"math"

	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/index"
	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

// BTreeIndex は leaf を保持する
// leaf は現在のインデックスレコードを track している
type BTreeIndex struct {
	tx         *tx.Transaction
	dirLayout  *record.Layout
	leafLayout *record.Layout
	dirTable   string
	leafTable  string
	leaf       *BTreeLeaf
	rootBlk    *file.BlockID
}

func NewBTreeIndex(tx *tx.Transaction, indexName string, leafLayout *record.Layout) (*BTreeIndex, error) {
	bti := &BTreeIndex{
		tx:         tx,
		leafLayout: leafLayout,
		leafTable:  fmt.Sprintf("%s_leaf", indexName),
		dirTable:   fmt.Sprintf("%s_directory", indexName),
	}

	if err := bti.initializeLeafTableIfNeeded(); err != nil {
		return nil, err
	}

	if err := bti.initializeDirectory(); err != nil {
		return nil, err
	}

	return bti, nil
}

// BeforeFirst はルートからブロックを探索して leaf を初期化する
func (bti *BTreeIndex) BeforeFirst(searchKey query.Constant) error {
	err := bti.Close()
	if err != nil {
		return err
	}
	rootDir, err := NewBTreeDirectory(bti.tx, bti.rootBlk, bti.dirLayout)
	if err != nil {
		return err
	}
	blkNum, err := rootDir.Search(searchKey)
	if err != nil {
		return err
	}
	// leaf を生成したら rootDir は不要なのでクローズする
	err = rootDir.Close()
	if err != nil {
		return err
	}
	leafBlk := file.NewBlockID(bti.leafTable, blkNum)
	leaf, err := NewBTreeLeaf(bti.tx, leafBlk, bti.leafLayout, searchKey)
	if err != nil {
		return err
	}
	bti.leaf = leaf
	return nil
}

func (bti *BTreeIndex) Next() (bool, error) {
	return bti.leaf.HasNext()
}

func (bti *BTreeIndex) GetDataRid() (*record.RecordID, error) {
	return bti.leaf.GetDataRid()
}

// Insert はインデックスレコードを追加し
// split された場合は新しい leaf のインデックスレコードを追加する
func (bti *BTreeIndex) Insert(dataVal query.Constant, rid *record.RecordID) error {
	if err := bti.BeforeFirst(dataVal); err != nil {
		return err
	}
	de, err := bti.leaf.Insert(rid)
	if err != nil {
		return err
	}
	if err := bti.leaf.Close(); err != nil {
		return err
	}
	if de.IsZero() {
		return nil
	}

	// leaf page が split された場合は、新しい leaf のインデックスレコードを追加する
	rootDir, err := NewBTreeDirectory(bti.tx, bti.rootBlk, bti.dirLayout)
	if err != nil {
		return err
	}
	e2, err := rootDir.Insert(de)
	if err != nil {
		return err
	}
	if !e2.IsZero() {
		err := rootDir.MakeNewRoot(e2)
		if err != nil {
			return err
		}
	}
	return rootDir.Close()
}

// Delete() は leaf からインデックスレコードを削除する
// ディレクトリの再構築は行わない
func (bti *BTreeIndex) Delete(dataVal query.Constant, rid *record.RecordID) error {
	if err := bti.BeforeFirst(dataVal); err != nil {
		return err
	}
	if err := bti.leaf.Delete(rid); err != nil {
		return err
	}
	return bti.leaf.Close()
}

func (bti *BTreeIndex) Close() error {
	if bti.leaf == nil {
		return nil
	}
	return bti.leaf.Close()
}

func (bti *BTreeIndex) initializeLeafTableIfNeeded() error {
	ls, err := bti.tx.Size(bti.leafTable)
	if err != nil {
		return err
	}
	if ls != 0 {
		return nil
	}

	blk, err := bti.tx.Append(bti.leafTable)
	if err != nil {
		return err
	}
	node, err := NewBTreePage(bti.tx, blk, bti.leafLayout)
	if err != nil {
		return err
	}
	err = node.Format(blk, NoOverFlow)
	if err != nil {
		return err
	}
	return nil
}

// initializeDirectory は leaf schema から対応する情報を取得して、同じスキーマを構築する
// 必要であれば root に最小値を入れることでフォーマットする
func (bti *BTreeIndex) initializeDirectory() error {
	dirSchema := record.NewSchema()
	if err := dirSchema.Add(index.IndexBlockNumberField, bti.leafLayout.Schema()); err != nil {
		return err
	}
	if err := dirSchema.Add(index.IndexDataValueField, bti.leafLayout.Schema()); err != nil {
		return err
	}

	dirLayout, err := record.NewLayout(dirSchema)
	if err != nil {
		return err
	}
	bti.dirLayout = dirLayout
	bti.rootBlk = file.NewBlockID(bti.dirTable, 0)

	ds, err := bti.tx.Size(bti.dirTable)
	if err != nil {
		return err
	}
	if ds != 0 {
		return nil
	}

	_, err = bti.tx.Append(bti.dirTable)
	if err != nil {
		return err
	}
	node, err := NewBTreePage(bti.tx, bti.rootBlk, bti.dirLayout)
	if err != nil {
		return err
	}
	if err := node.Format(bti.rootBlk, 0); err != nil {
		return err
	}

	// insert initial directory entry
	ft, err := dirSchema.FieldType(index.IndexDataValueField)
	if err != nil {
		return err
	}

	var minVal query.Constant
	switch ft {
	case record.Integer:
		minVal = query.NewConstant(0)
	case record.String:
		minVal = query.NewConstant("")
	default:
		return fmt.Errorf("invalid field type %v", ft)
	}

	if err := node.insertDirectory(0, minVal, 0); err != nil {
		return err
	}
	if err := node.Close(); err != nil {
		return err
	}
	return nil
}

func SearchCost(numBlocks int, rpb int) int {
	return int(math.Log(float64(numBlocks)) / math.Log(float64(rpb)))
}
