package file

import "strconv"

type BlockID struct {
	filename string
	blknum   int
}

func NewBlockID(filename string, blknum int) *BlockID {
	return &BlockID{filename, blknum}
}

func (b BlockID) FileName() string {
	return b.filename
}

func (b BlockID) Number() int {
	return b.blknum
}

func (b1 BlockID) Equals(b2 *BlockID) bool {
	return b1.filename == b2.filename && b1.blknum == b2.blknum
}

func (b BlockID) String() string {
	return "[file " + b.filename + ", block " + strconv.Itoa(b.blknum) + "]"
}

// p.69 hashcodeは必要か？
