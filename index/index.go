package index

type Index interface {
	Next() bool
}

type IndexType uint8

const (
	HashIndexType IndexType = iota + 1
)

func SearchCost(it IndexType, numBlocks int, rpb int) int {
	return 0
}
