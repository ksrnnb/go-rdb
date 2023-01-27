package btree

import "github.com/ksrnnb/go-rdb/query"

var emptyDir = DirectoryEntry{}

type DirectoryEntry struct {
	dataVal query.Constant
	blkNum  int
}

func NewDirectoryEntry(dataVal query.Constant, blkNum int) DirectoryEntry {
	return DirectoryEntry{dataVal, blkNum}
}

func (de DirectoryEntry) DataValue() query.Constant {
	return de.dataVal
}

func (de DirectoryEntry) BlockNumber() int {
	return de.blkNum
}

func (de DirectoryEntry) IsZero() bool {
	return de == DirectoryEntry{}
}
