package datastruct_test

import (
	"strconv"
	"testing"

	"github.com/IfanTsai/metis/datastruct"
	"github.com/stretchr/testify/require"
)

func TestSkiplist_Insert(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	require.Equal(t, int64(10000), s.Length)
}

func TestSkiplist_Delete(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	for i := 1; i <= 10000; i++ {
		s.Delete(float64(i), "value"+strconv.Itoa(i))
	}

	require.Equal(t, int64(0), s.Length)
}

func TestSkiplist_GetRank(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	for i := 1; i <= 10000; i++ {
		rank := s.GetRank(float64(i), "value"+strconv.Itoa(i))
		require.Equal(t, int64(i), rank)
	}
}

func TestSkiplist_GetElementByRank(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	for i := 1; i <= 10000; i++ {
		node := s.GetElementByRank(int64(i))
		require.Equal(t, float64(i), node.Score)
		require.Equal(t, "value"+strconv.Itoa(i), node.Value)
	}
}
