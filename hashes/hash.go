package hashes

import (
	"fmt"
	"hash/fnv"
)

var h = fnv.New32a()

func HashCode(v interface{}) uint32 {
	b := []byte(fmt.Sprintf("%v", v))
	h.Write(b)
	return h.Sum32()
}
