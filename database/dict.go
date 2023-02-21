package database

import (
	"hash/fnv"

	"github.com/IfanTsai/go-lib/utils/byteutils"
)

type dictType struct{}

func (d *dictType) Hash(a any) int64 {
	hash := fnv.New64a()
	hash.Write(byteutils.S2B(a.(string)))

	return int64(hash.Sum64())
}

func (d *dictType) Equal(a, b any) bool {
	return a == b
}
