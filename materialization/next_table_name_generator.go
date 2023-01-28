package materialization

import (
	"fmt"
	"sync"
)

type NextTableNameGenerator struct {
	nextTableNum int
	mux          sync.Mutex
}

func NewNextTableNameGenerator() *NextTableNameGenerator {
	return &NextTableNameGenerator{nextTableNum: 0, mux: sync.Mutex{}}
}

func (g *NextTableNameGenerator) NextTableName() string {
	g.mux.Lock()
	defer g.mux.Unlock()

	g.nextTableNum++
	return fmt.Sprintf("temporary_table_%d", g.nextTableNum)
}
