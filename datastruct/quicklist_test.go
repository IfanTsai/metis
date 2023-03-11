package datastruct_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/IfanTsai/metis/datastruct"
	"github.com/stretchr/testify/require"
)

func TestQuicklist_PushBack(t *testing.T) {
	q := datastruct.NewQuicklist()
	for i := 0; i < 10000; i++ {
		q.PushBack("value" + strconv.Itoa(i))
	}

	iter := datastruct.NewQuicklistIterator(q)
	for i := 0; i < 10000; i++ {
		require.Equal(t, "value"+strconv.Itoa(i), iter.Next().(string))
	}
}

func TestQuicklist_Remove(t *testing.T) {
	q := datastruct.NewQuicklist()
	for i := 0; i < 10000; i++ {
		q.PushBack("value" + strconv.Itoa(i))
	}

	require.Equal(t, 10000, q.Len())

	iter := datastruct.NewQuicklistIterator(q)
	for i := 0; i < 10000; i++ {
		require.Equal(t, "value"+strconv.Itoa(i), iter.Next().(string))
	}

	fmt.Println(q.Len())
	for i := 0; i < 10000; i++ {
		q.Remove(0)
	}

	require.Equal(t, 0, q.Len())

	iter = datastruct.NewQuicklistIterator(q)
	require.Nil(t, iter.Next())
}

func TestQuicklist_Find(t *testing.T) {
	q := datastruct.NewQuicklist()
	for i := 0; i < 10000; i++ {
		q.PushBack("value" + strconv.Itoa(i))
	}

	for i := 0; i < 10000; i++ {
		require.Equal(t, i, q.Find("value"+strconv.Itoa(i)))
	}
}

func TestQuicklist_Insert(t *testing.T) {
	q := datastruct.NewQuicklist()
	for i := 0; i < 10000; i++ {
		q.PushBack("value" + strconv.Itoa(i))
	}

	for i := 0; i < 10000; i++ {
		q.Insert(i, "value"+strconv.Itoa(i+10000))
		require.Equal(t, i, q.Find("value"+strconv.Itoa(i+10000)))
	}
}

func TestQuicklist_Range(t *testing.T) {
	q := datastruct.NewQuicklist()
	for i := 0; i < 10000; i++ {
		q.PushBack("value" + strconv.Itoa(i))
	}

	values := q.Range(0, 9999)
	require.Equal(t, 10000, len(values))
	for i := 0; i < 10000; i++ {
		require.Equal(t, "value"+strconv.Itoa(i), values[i])
	}

}
