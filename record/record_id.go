package record

import "fmt"

type RecordID struct {
	blknum int
	slot   int
}

func NewRecordID(blknum, slot int) *RecordID {
	return &RecordID{blknum, slot}
}

func (r *RecordID) BlockNumber() int {
	return r.blknum
}

func (r *RecordID) Slot() int {
	return r.slot
}

func (r *RecordID) Equals(rr *RecordID) bool {
	return r.blknum == rr.blknum && r.slot == rr.slot
}

func (r *RecordID) String() string {
	return fmt.Sprintf("[%d, %d]", r.blknum, r.slot)
}
