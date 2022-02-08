package iterator

type ByteIterator interface {
	HasNext() bool
	Next() ([]byte, error)
}
