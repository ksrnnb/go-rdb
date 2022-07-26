package testing

import (
	"testing"

	"github.com/ksrnnb/go-rdb/server"
)

func NewSimpleDB(t *testing.T, dirname string, blockSize, bufferSize int) *server.SimpleDB {
	return server.NewSimpleDB(dirname, blockSize, bufferSize)
}
