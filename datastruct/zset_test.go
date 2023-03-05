package datastruct_test

import (
	"math"
	"strconv"
	"testing"

	"github.com/IfanTsai/metis/datastruct"
	"github.com/stretchr/testify/require"
)

func TestZset_Add(t *testing.T) {
	t.Parallel()

	zset := datastruct.NewZset(&dictType{})

	for i := 0; i < 100; i++ {
		require.True(t, zset.Add(float64(i), "value"+strconv.Itoa(i)))
	}

	for i := 0; i < 100; i++ {
		require.False(t, zset.Add(float64(i), "value"+strconv.Itoa(i)))
	}
}

func TestZset_Get(t *testing.T) {
	t.Parallel()

	zset := datastruct.NewZset(&dictType{})

	for i := 0; i < 100; i++ {
		zset.Add(float64(i), "value"+strconv.Itoa(i))
	}

	for i := 0; i < 100; i++ {
		require.Equal(t, float64(i), zset.Get("value"+strconv.Itoa(i)).Score)
	}
}

func TestZset_Delete(t *testing.T) {
	t.Parallel()

	zset := datastruct.NewZset(&dictType{})

	for i := 0; i < 100; i++ {
		zset.Add(float64(i), "value"+strconv.Itoa(i))
	}

	for i := 0; i < 100; i++ {
		require.True(t, zset.Delete("value"+strconv.Itoa(i)))
	}

	for i := 0; i < 100; i++ {
		require.False(t, zset.Delete("value"+strconv.Itoa(i)))
	}
}

func TestZset_DeleteRangeByRank(t *testing.T) {
	t.Parallel()

	zset := datastruct.NewZset(&dictType{})

	for i := 0; i < 100; i++ {
		zset.Add(float64(i), "value"+strconv.Itoa(i))
	}

	zset.DeleteRangeByRank(0, 49)
	require.Equal(t, int64(50), zset.Size())

	zset.DeleteRangeByRank(0, math.MaxInt64)
	require.Equal(t, int64(0), zset.Size())
}

func TestZset_DeleteRangeByScore(t *testing.T) {
	t.Parallel()

	zset := datastruct.NewZset(&dictType{})

	for i := 0; i < 100; i++ {
		zset.Add(float64(i), "value"+strconv.Itoa(i))
	}

	zset.DeleteRangeByScore(0, 49)
	require.Equal(t, int64(50), zset.Size())

	zset.DeleteRangeByScore(0, math.MaxInt64)
	require.Equal(t, int64(0), zset.Size())
}

func TestZset_GetRank(t *testing.T) {
	t.Parallel()

	zset := datastruct.NewZset(&dictType{})

	for i := 0; i < 100; i++ {
		zset.Add(float64(i), "value"+strconv.Itoa(i))
	}

	for i := 0; i < 100; i++ {
		require.Equal(t, int64(i), zset.GetRank("value"+strconv.Itoa(i), false))
	}

	for i := 0; i < 100; i++ {
		require.Equal(t, int64(99-i), zset.GetRank("value"+strconv.Itoa(i), true))
	}
}

func TestZset_RangeByRank(t *testing.T) {
	t.Parallel()

	zset := datastruct.NewZset(&dictType{})

	for i := 0; i < 100; i++ {
		zset.Add(float64(i), "value"+strconv.Itoa(i))
	}

	for i := 0; i < 100; i++ {
		for j := i + 1; j < 100; j++ {
			elements := zset.RangeByRank(int64(i), int64(j), false)
			require.Equal(t, j-i+1, len(elements))

			for k, element := range elements {
				require.Equal(t, float64(i+k), element.Score)
				require.Equal(t, "value"+strconv.Itoa(i+k), element.Member)
			}
		}
	}

	for i := 0; i < 100; i++ {
		for j := i + 1; j < 100; j++ {
			elements := zset.RangeByRank(int64(i), int64(j), true)
			require.Equal(t, j-i+1, len(elements))

			for k, element := range elements {
				require.Equal(t, float64(99-i-k), element.Score)
				require.Equal(t, "value"+strconv.Itoa(99-i-k), element.Member)
			}
		}
	}
}

func TestZset_RangeByScore(t *testing.T) {
	t.Parallel()

	zset := datastruct.NewZset(&dictType{})

	for i := 0; i < 100; i++ {
		zset.Add(float64(i), "value"+strconv.Itoa(i))
	}

	for i := 0; i < 100; i++ {
		for j := i + 1; j < 100; j++ {
			elements := zset.RangeByScore(float64(i), float64(j), -1, false)
			require.Equal(t, j-i+1, len(elements))

			for k, element := range elements {
				require.Equal(t, float64(i+k), element.Score)
				require.Equal(t, "value"+strconv.Itoa(i+k), element.Member)
			}
		}
	}

	for i := 0; i < 100; i++ {
		for j := i + 1; j < 100; j++ {
			elements := zset.RangeByScore(float64(i), float64(j), -1, true)
			require.Equal(t, j-i+1, len(elements))

			for k, element := range elements {
				require.Equal(t, float64(j-k), element.Score)
				require.Equal(t, "value"+strconv.Itoa(j-k), element.Member)
			}
		}
	}

	for i := 0; i < 100; i++ {
		for j := i + 1; j < 100; j++ {
			for k := 0; k < 10; k++ {
				elements := zset.RangeByScore(float64(i), float64(j), int64(k), false)
				require.Equal(t, int(math.Min(float64(k), float64(j-i+1))), len(elements))

				for l, element := range elements {
					require.Equal(t, float64(i+l), element.Score)
					require.Equal(t, "value"+strconv.Itoa(i+l), element.Member)
				}
			}
		}
	}
}
