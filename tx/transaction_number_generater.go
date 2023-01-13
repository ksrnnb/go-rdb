package tx

import "sync"

type TransactionNumberGenerator struct {
	mux *sync.Mutex
	num int
}

func NewTransactionNumberGenerator() *TransactionNumberGenerator {
	return &TransactionNumberGenerator{
		mux: &sync.Mutex{},
		num: 0,
	}
}

func (tng *TransactionNumberGenerator) nextTxNumber() int {
	tng.mux.Lock()
	defer tng.mux.Unlock()

	tng.num++
	return tng.num
}
