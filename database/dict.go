package database

import (
	"hash/fnv"

	"github.com/IfanTsai/metis/datastruct"
)

type dictType struct{}

func (d *dictType) Hash(a *datastruct.Object) int64 {
	if a.Type != datastruct.ObjectTypeString {
		return 0
	}

	hash := fnv.New64a()
	hash.Write([]byte(a.Value.(string)))

	return int64(hash.Sum64())
}

func (d *dictType) Equal(a, b *datastruct.Object) bool {
	if a.Type != datastruct.ObjectTypeString || b.Type != datastruct.ObjectTypeString {
		return false
	}

	return a.Value.(string) == b.Value.(string)
}
