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

func TestSkiplist_DeleteRangeByScore(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	s.DeleteRangeByScore(1, 500)
	require.Equal(t, int64(9500), s.Length)

	s.DeleteRangeByScore(500, 10000)
	require.Equal(t, int64(0), s.Length)
}

func TestSkiplist_DeleteRangeByRank(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	s.DeleteRangeByRank(1, 500)
	require.Equal(t, int64(9500), s.Length)

	s.DeleteRangeByRank(1, 9500)
	require.Equal(t, int64(0), s.Length)
}

func TestSkiplist_Count(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	require.Equal(t, int64(500), s.Count(500, 999))
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
		require.Equal(t, "value"+strconv.Itoa(i), node.Member)
	}
}

func TestSkiplist_HasInScoreRange(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	require.True(t, s.HasInScoreRange(1, 10000))
	require.False(t, s.HasInScoreRange(10001, 20000))
}

func TestSkiplist_GetFirstInScoreRange(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	node := s.GetFirstInScoreRange(1, 10000)
	require.Equal(t, float64(1), node.Score)
	require.Equal(t, "value1", node.Member)
}

func TestSkiplist_GetLastInScoreRange(t *testing.T) {
	t.Parallel()

	s := datastruct.NewSkiplist()

	for i := 1; i <= 10000; i++ {
		s.Insert(float64(i), "value"+strconv.Itoa(i))
	}

	node := s.GetLastInScoreRange(1, 10000)
	require.Equal(t, float64(10000), node.Score)
	require.Equal(t, "value10000", node.Member)
}
