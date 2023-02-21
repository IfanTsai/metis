package datastruct_test

import (
	"hash/fnv"
	"math"
	"strconv"
	"testing"

	"github.com/IfanTsai/go-lib/utils/byteutils"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/stretchr/testify/require"
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

func BenchmarkDict_Set(b *testing.B) {
	dict := datastruct.NewDict(&dictType{})

	b.ResetTimer()
	for i := 1; i <= 10000; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		dict.Set(key, val)
	}
}

func BenchmarkDict_Get(b *testing.B) {
	dict := datastruct.NewDict(&dictType{})

	for i := 1; i <= 10000; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		dict.Set(key, val)
	}

	b.ResetTimer()

	for i := 1; i <= 10000; i++ {
		key := "key" + strconv.Itoa(i)
		dict.Get(key)
	}
}

func BenchmarkDict_Delete(b *testing.B) {
	dict := datastruct.NewDict(&dictType{})

	for i := 1; i <= 10000; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		dict.Set(key, val)
	}

	b.ResetTimer()

	for i := 1; i <= 10000; i++ {
		key := "key" + strconv.Itoa(i)
		dict.Delete(key)
	}
}

func TestDict_Find(t *testing.T) {
	dict := datastruct.NewDict(&dictType{})

	k1 := "foo"
	v1 := "bar"

	dict.Set(k1, v1)
	entry := dict.Find(k1)
	require.Equal(t, "bar", entry.Value.(string))

	k2 := "qux"
	entry = dict.Find(k2)
	require.Nil(t, entry)

	v2 := "baz"
	dict.Set(k1, v2)
	entry = dict.Find(k1)
	require.Equal(t, "baz", entry.Value.(string))
}

func TestDict_SetGet(t *testing.T) {
	dict := datastruct.NewDict(&dictType{})

	for i := 1; i <= 1000; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		dict.Set(key, val)
		require.Equal(t, int64(i), dict.Size())
	}

	for i := 1; i <= 1000; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		obj := dict.Get(key)
		require.NotNil(t, obj)
		require.Equal(t, val, obj.(string))
	}

	for i := 1001; i <= 2000; i++ {
		key := "key" + strconv.Itoa(i)
		require.Nil(t, dict.Get(key))
	}
}

func TestDict_GetRandomKey(t *testing.T) {
	dict := datastruct.NewDict(&dictType{})

	for i := 1; i <= 1000000; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		dict.Set(key, val)
	}

	keys := make(map[string]struct{})
	for i := 1; i <= 100; i++ {
		entry := dict.GetRandomKey()
		require.NotNil(t, entry)
		keys[entry.Key.(string)] = struct{}{}
	}

	require.Equal(t, 100, len(keys))
}

func TestDict_Delete(t *testing.T) {
	dict := datastruct.NewDict(&dictType{})

	k1 := "foo"
	v1 := "bar"

	require.ErrorIs(t, dict.Delete(k1), datastruct.ErrNotInitialized)

	dict.Set(k1, v1)
	require.Equal(t, "bar", dict.Get(k1).(string))
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

func TestDictIterator_Next(t *testing.T) {
	dict := datastruct.NewDict(&dictType{})

	expectTestCaseMap := make(map[string]string)
	for i := 1; i <= 1000; i++ {
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		dict.Set(key, val)
		expectTestCaseMap[key] = val
	}

	iter := datastruct.NewDictIterator(dict)
	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		require.Equal(t, expectTestCaseMap[entry.Key.(string)], entry.Value.(string))
		delete(expectTestCaseMap, entry.Key.(string))
	}

	require.Empty(t, expectTestCaseMap)
}
