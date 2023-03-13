package datastruct_test

import (
	"strconv"
	"testing"

	"github.com/IfanTsai/metis/datastruct"
	"github.com/stretchr/testify/require"
)

func TestSet_Add(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSet(&dictType{})
	for i := 0; i < 10000; i++ {
		s.Add("key" + strconv.Itoa(i))
	}

	require.Equal(t, int64(10000), s.Size())
}

func TestSet_Remove(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSet(&dictType{})
	for i := 0; i < 10000; i++ {
		s.Add("key" + strconv.Itoa(i))
	}

	for i := 0; i < 10000; i++ {
		require.NoError(t, s.Delete("key"+strconv.Itoa(i)))
	}

	require.Equal(t, int64(0), s.Size())
}

func TestSet_Contains(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSet(&dictType{})
	for i := 0; i < 10000; i++ {
		s.Add("key" + strconv.Itoa(i))
	}

	for i := 0; i < 10000; i++ {
		require.True(t, s.Contains("key"+strconv.Itoa(i)))
	}
}

func TestSet_Union(t *testing.T) {
	t.Parallel()

	s1 := datastruct.NewSet(&dictType{})
	for i := 0; i < 10000; i++ {
		s1.Add("key" + strconv.Itoa(i))
	}

	s2 := datastruct.NewSet(&dictType{})
	for i := 10000; i < 20000; i++ {
		s2.Add("key" + strconv.Itoa(i))
	}

	s3 := s1.Union(s2)
	require.Equal(t, int64(20000), s3.Size())

	for i := 0; i < 20000; i++ {
		require.True(t, s3.Contains("key"+strconv.Itoa(i)))
	}
}

func TestSet_Intersect(t *testing.T) {
	t.Parallel()

	s1 := datastruct.NewSet(&dictType{})
	for i := 0; i < 10000; i++ {
		s1.Add("key" + strconv.Itoa(i))
	}

	s2 := datastruct.NewSet(&dictType{})
	for i := 5000; i < 20000; i++ {
		s2.Add("key" + strconv.Itoa(i))
	}

	s3 := s1.Intersect(s2)
	require.Equal(t, int64(5000), s3.Size())

	for i := 5000; i < 10000; i++ {
		require.True(t, s3.Contains("key"+strconv.Itoa(i)))
	}

	for i := 0; i < 5000; i++ {
		require.False(t, s3.Contains("key"+strconv.Itoa(i)))
	}

	for i := 10000; i < 20000; i++ {
		require.False(t, s3.Contains("key"+strconv.Itoa(i)))
	}
}

func TestSet_Difference(t *testing.T) {
	t.Parallel()

	s1 := datastruct.NewSet(&dictType{})
	for i := 0; i < 10000; i++ {
		s1.Add("key" + strconv.Itoa(i))
	}

	s2 := datastruct.NewSet(&dictType{})
	for i := 5000; i < 20000; i++ {
		s2.Add("key" + strconv.Itoa(i))
	}

	s3 := s1.Difference(s2)
	require.Equal(t, int64(5000), s3.Size())

	for i := 0; i < 5000; i++ {
		require.True(t, s3.Contains("key"+strconv.Itoa(i)))
	}

	for i := 5000; i < 10000; i++ {
		require.False(t, s3.Contains("key"+strconv.Itoa(i)))
	}
}
