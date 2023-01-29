package planner

import (
	"errors"
	"math"

	"github.com/ksrnnb/go-rdb/query"
	"github.com/ksrnnb/go-rdb/record"
	"github.com/ksrnnb/go-rdb/tx"
)

type MultiBufferProductScan struct {
	tx                              *tx.Transaction
	lhs, rhs, prodScan              query.Scanner
	fileName                        string
	layout                          *record.Layout
	chunkSize, nextBlkNum, fileSize int
}

func NewMultiBufferProductScan(tx *tx.Transaction, lhs query.Scanner, fileName string, layout *record.Layout) (*MultiBufferProductScan, error) {
	ms := &MultiBufferProductScan{
		tx:       tx,
		lhs:      lhs,
		fileName: fileName,
		layout:   layout,
	}
	var err error
	ms.fileSize, err = tx.Size(ms.fileName)
	if err != nil {
		return nil, err
	}
	available := tx.AvailableBuffers()
	ms.chunkSize = BestFactor(available, ms.fileSize)

	if err := ms.BeforeFirst(); err != nil {
		return nil, err
	}
	return ms, nil
}

func (ms *MultiBufferProductScan) BeforeFirst() error {
	ms.nextBlkNum = 0
	_, err := ms.useNextChunk()
	return err
}

func (ms *MultiBufferProductScan) Next() (bool, error) {
	if ms.prodScan == nil {
		return false, errors.New("Next() failed: MultiBufferProductScan.prodScan is nil")
	}

	hasNext, err := ms.prodScan.Next()
	if err != nil {
		return false, err
	}
	for !hasNext {
		nextChunk, err := ms.useNextChunk()
		if err != nil {
			return false, err
		}
		if !nextChunk {
			return false, nil
		}
	}
	return true, nil
}

func (ms *MultiBufferProductScan) Close() error {
	return ms.prodScan.Close()
}

func (ms *MultiBufferProductScan) GetInt(fieldName string) (int, error) {
	return ms.prodScan.GetInt(fieldName)
}

func (ms *MultiBufferProductScan) GetString(fieldName string) (string, error) {
	return ms.prodScan.GetString(fieldName)
}

func (ms *MultiBufferProductScan) GetVal(fieldName string) (query.Constant, error) {
	return ms.prodScan.GetVal(fieldName)
}

func (ms *MultiBufferProductScan) HasField(fieldName string) bool {
	return ms.prodScan.HasField(fieldName)
}

func (ms *MultiBufferProductScan) useNextChunk() (bool, error) {
	if ms.rhs != nil {
		if err := ms.rhs.Close(); err != nil {
			return false, err
		}
	}
	if ms.nextBlkNum >= ms.fileSize {
		return false, nil
	}
	end := ms.nextBlkNum + ms.chunkSize - 1
	if end >= ms.fileSize {
		end = ms.fileSize - 1
	}

	ms.rhs = NewChunkScan(ms.tx, ms.fileName, ms.layout, ms.nextBlkNum, end)
	if err := ms.lhs.BeforeFirst(); err != nil {
		return false, err
	}
	prod, err := query.NewProductScan(ms.lhs, ms.rhs)
	if err != nil {
		return false, err
	}
	ms.prodScan = prod
	ms.nextBlkNum = end + 1
	return true, nil
}

func BestRoot(available int, size int) int {
	// reserve a couple of buffers
	avail := available - 2
	if avail <= 1 {
		return 1
	}
	k := math.MaxInt
	i := 1
	for k > avail {
		i++
		k = int(math.Ceil(math.Pow(float64(size), 1.0/float64(i))))
	}
	return k
}

func BestFactor(available, size int) int {
	// reserve a couple of buffers
	avail := available - 2
	if avail <= 1 {
		return 1
	}
	k := size
	i := 1
	for k > avail {
		i++
		k = int(math.Ceil(float64(size) / float64(i)))
	}
	return k
}
