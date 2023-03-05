package datastruct

import (
	"math"
)

type ZsetElement struct {
	Member string
	Score  float64
}

type Zset struct {
	dict     *Dict     // member -> score
	skiplist *Skiplist // used to maintain the order of members and range queries
}

func NewZset(dictType DictType) *Zset {
	return &Zset{
		dict:     NewDict(dictType),
		skiplist: NewSkiplist(),
	}
}

// Add adds a new member or updates the score of an existing member.
// Returns true if the element is new, false otherwise.
func (z *Zset) Add(score float64, member string) bool {
	element := z.Get(member)
	if element != nil {
		if score != element.Score {
			element.Score = score
			z.skiplist.Delete(element.Score, element.Member)
			z.skiplist.Insert(score, member)
		}

		return false
	}

	z.dict.Set(member, &ZsetElement{member, score})
	z.skiplist.Insert(score, member)

	return true
}

// Get returns the element for given member or nil if the member is not exist.
func (z *Zset) Get(member string) *ZsetElement {
	entry := z.dict.Find(member)
	if entry == nil {
		return nil
	}

	return entry.Value.(*ZsetElement)
}

// Delete removes the given member from the zset. Returns true if the member is removed, false otherwise.
func (z *Zset) Delete(member string) bool {
	element := z.Get(member)
	if element == nil {
		return false
	}

	z.dict.Delete(member)
	z.skiplist.Delete(element.Score, member)

	return true
}

// DeleteRangeByRank removes all elements with rank between start and end.
// Note that rank is 0-based.
func (z *Zset) DeleteRangeByRank(start, end int64) []*ZsetElement {
	if end == math.MaxInt64 {
		end = z.skiplist.Length - 1
	}

	nodes := z.skiplist.DeleteRangeByRank(start+1, end+1)
	elements := make([]*ZsetElement, len(nodes))

	for i, node := range nodes {
		elements[i] = z.Get(node.Member)
		z.dict.Delete(node.Member)
	}

	return elements
}

// DeleteRangeByScore removes all elements with score between min and max (inclusive).
func (z *Zset) DeleteRangeByScore(min, max float64) []*ZsetElement {
	nodes := z.skiplist.DeleteRangeByScore(min, max)
	elements := make([]*ZsetElement, len(nodes))

	for i, node := range nodes {
		elements[i] = z.Get(node.Member)
		z.dict.Delete(node.Member)
	}

	return elements
}

// GetRank returns the 0-based rank of the member or -1 if the member is not exist.
// The rank is calculated from the lowest to the highest Score.
// If reverse is true, the rank is calculated from the highest to the lowest Score.
func (z *Zset) GetRank(member string, reverse bool) int64 {
	element := z.Get(member)
	if element == nil {
		return -1
	}

	rank := z.skiplist.GetRank(element.Score, member)
	if reverse {
		return z.skiplist.Length - rank
	}

	return rank - 1
}

// RangeByRank returns a slice of elements with rank between start and end.
// The rank is 0-based.
func (z *Zset) RangeByRank(start, end int64, reverse bool) []*ZsetElement {
	if end == math.MaxInt64 {
		end = z.skiplist.Length - 1
	}

	nodes := z.skiplist.RangeByRank(start+1, end+1, reverse)
	elements := make([]*ZsetElement, len(nodes))
	for i, node := range nodes {
		elements[i] = z.Get(node.Member)
	}

	return elements
}

// RangeByScore returns a slice of elements with score between min and max.
func (z *Zset) RangeByScore(min, max float64, limit int64, reverse bool) []*ZsetElement {
	nodes := z.skiplist.RangeByScore(min, max, limit, reverse)
	elements := make([]*ZsetElement, len(nodes))
	for i, node := range nodes {
		elements[i] = z.Get(node.Member)
	}

	return elements
}

// Size returns the number of elements in the zset.
func (z *Zset) Size() int64 {
	return z.skiplist.Length
}

// Count returns the number of elements with score between min and max.
func (z *Zset) Count(min, max float64) int64 {
	return z.skiplist.Count(min, max)
}
