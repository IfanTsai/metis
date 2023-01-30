package datastruct_test

import (
	"fmt"
	"hash/fnv"
	"math"
	"testing"

	"github.com/IfanTsai/metis/datastruct"
	"github.com/stretchr/testify/require"
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

func BenchmarkDict_Set(b *testing.B) {
	dict := datastruct.NewDict(&dictType{})

	b.ResetTimer()
	for i := 1; i <= 10000; i++ {
		key := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("key%d", i))
		val := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("value%d", i))
		dict.Set(key, val)
	}
}

func BenchmarkDict_Get(b *testing.B) {
	dict := datastruct.NewDict(&dictType{})

	for i := 1; i <= 10000; i++ {
		key := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("key%d", i))
		val := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("value%d", i))
		dict.Set(key, val)
	}

	b.ResetTimer()

	for i := 1; i <= 10000; i++ {
		key := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("key%d", i))
		dict.Get(key)
	}
}

func BenchmarkDict_Delete(b *testing.B) {
	dict := datastruct.NewDict(&dictType{})

	for i := 1; i <= 10000; i++ {
		key := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("key%d", i))
		val := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("value%d", i))
		dict.Set(key, val)
	}

	b.ResetTimer()

	for i := 1; i <= 10000; i++ {
		key := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("key%d", i))
		dict.Delete(key)
	}
}

func TestDict_Find(t *testing.T) {
	dict := datastruct.NewDict(&dictType{})

	k1 := datastruct.NewObject(datastruct.ObjectTypeString, "foo")
	v1 := datastruct.NewObject(datastruct.ObjectTypeString, "bar")

	dict.Set(k1, v1)
	entry := dict.Find(k1)
	require.Equal(t, "bar", entry.Value.Value.(string))

	k2 := datastruct.NewObject(datastruct.ObjectTypeString, "qux")
	entry = dict.Find(k2)
	require.Nil(t, entry)

	v2 := datastruct.NewObject(datastruct.ObjectTypeString, "baz")
	dict.Set(k1, v2)
	entry = dict.Find(k1)
	require.Equal(t, "baz", entry.Value.Value.(string))
}

func TestDict_SetGet(t *testing.T) {
	dict := datastruct.NewDict(&dictType{})

	for i := 1; i <= 1000; i++ {
		key := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("key%d", i))
		val := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("value%d", i))
		dict.Set(key, val)
		require.Equal(t, int64(i), dict.Size())
	}

	for i := 1; i <= 1000; i++ {
		key := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("key%d", i))
		val := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("value%d", i))
		obj := dict.Get(key)
		require.NotNil(t, obj)
		require.Equal(t, val.Value.(string), obj.Value.(string))
	}

	for i := 1001; i <= 2000; i++ {
		k := datastruct.NewObject(datastruct.ObjectTypeString, fmt.Sprintf("key%d", i))
		require.Nil(t, dict.Get(k))
	}
}

func TestDict_Delete(t *testing.T) {
	dict := datastruct.NewDict(&dictType{})

	k1 := datastruct.NewObject(datastruct.ObjectTypeString, "foo")
	v1 := datastruct.NewObject(datastruct.ObjectTypeString, "bar")

	require.ErrorIs(t, dict.Delete(k1), datastruct.ErrNotInitialized)

	dict.Set(k1, v1)
	require.Equal(t, "bar", dict.Get(k1).Value.(string))
	require.NoError(t, dict.Delete(k1))

	require.ErrorIs(t, dict.Delete(k1), datastruct.ErrKeyNotFound)
}

func TestGetNextPower(t *testing.T) {
	testCases := []struct {
		input    int64
		expected int64
	}{
		{-1, 4},
		{0, 4},
		{1, 4},
		{2, 4},
		{3, 4},
		{4, 4},
		{5, 8},
		{6, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{16, 16},
		{17, 32},
		{31, 32},
		{32, 32},
		{math.MaxInt32 - 1, math.MaxInt32 + 1},
		{math.MaxInt32, math.MaxInt32 + 1},
		{math.MaxInt64 - 1, math.MaxInt64},
		{math.MaxInt64, math.MaxInt64},
	}

	for index := range testCases {
		testCase := testCases[index]
		require.Equal(t, testCase.expected, datastruct.GetNextPower(testCase.input))
	}
}
