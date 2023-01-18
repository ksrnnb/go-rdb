package server

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/ksrnnb/go-rdb/buffer"
	"github.com/ksrnnb/go-rdb/file"
	"github.com/ksrnnb/go-rdb/logs"
	"github.com/ksrnnb/go-rdb/tx"
	"github.com/ksrnnb/go-rdb/tx/concurrency"
)

type SimpleDB struct {
	fm  *file.FileManager
	lm  *logs.LogManager
	bm  *buffer.BufferManager
	lt  *concurrency.LockTable
	tng *tx.TransactionNumberGenerator
}

const blockSize = 400
const logFile = "simpledb.log"

func NewSimpleDB(dirname string, blockSize, bufferSize int) *SimpleDB {
	dbDirectory, err := createDirectoryIfNeeded(dirname)

	if err != nil {
		log.Fatalf("NewSimpleDB() failed, %v", err)
	}

	fm, err := file.NewFileManager(dbDirectory, blockSize)

	if err != nil {
		log.Fatalf("NewSimpleDB() failed, %v", err)
	}

	lm, err := logs.NewLogManager(fm, logFile)

	if err != nil {
		log.Fatalf("NewSimpleDB() failed, %v", err)
	}

	bm := buffer.NewBufferManager(fm, lm, bufferSize)

	return &SimpleDB{
		fm:  fm,
		lm:  lm,
		bm:  bm,
		lt:  concurrency.NewLockTable(),
		tng: tx.NewTransactionNumberGenerator(),
	}
}

func (db *SimpleDB) FileManager() *file.FileManager {
	return db.fm
}

func (db *SimpleDB) LogManager() *logs.LogManager {
	return db.lm
}

func (db *SimpleDB) BufferManager() *buffer.BufferManager {
	return db.bm
}

func (db *SimpleDB) NewTransaction() (*tx.Transaction, error) {
	return tx.NewTransaction(db.fm, db.lm, db.bm, db.lt, db.tng)
}

// createDirectoryIfNeeded()はディレクトリ名を引数にとり、
// 兄弟となる階層にディレクトリが存在しなければ作成、存在すればパスを返す
func createDirectoryIfNeeded(dirname string) (dbDirectory string, err error) {
	dbDirectory = filepath.Join("./..", dirname)

	_, err = os.Stat(dbDirectory)
	if os.IsNotExist(err) {
		err = os.Mkdir(dbDirectory, 0744)

		if err != nil {
			return "", fmt.Errorf("newDirectory() failed, %v", err)
		}
	}

	if err != nil {
		return "", err
	}

	return dbDirectory, nil
}
