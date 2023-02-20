package datastruct

import (
	"math/rand"
)

const (
	maxLevel    = 32
	probiablity = 0.25
)

type Skiplist struct {
	Head, Tail *SkiplistNode
	Length     int64
	Level      int8
}

type SkiplistNode struct {
	Member   string
	Score    float64
	Backward *SkiplistNode
	Levels   []struct {
		Forward *SkiplistNode
		Span    int64
	}
}

func NewSkiplist() *Skiplist {
	return &Skiplist{
		Head:   newSkiplistNode(maxLevel, 0, ""),
		Tail:   nil,
		Length: 0,
		Level:  1,
	}
}

// Insert inserts the new node with the specified score and member into the skiplist.
// Assumes the element does not already exist in the skiplist (up to the caller to enforce that).
func (s *Skiplist) Insert(score float64, member string) *SkiplistNode {
	update := make([]*SkiplistNode, maxLevel)
	rank := make([]int64, maxLevel)

	x := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		if i == s.Level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.Levels[i].Forward != nil &&
			(x.Levels[i].Forward.Score < score ||
				(x.Levels[i].Forward.Score == score && x.Levels[i].Forward.Member < member)) {
			rank[i] += x.Levels[i].Span
			x = x.Levels[i].Forward
		}

		update[i] = x
	}

	level := randomLevel()
	if level > s.Level {
		for i := s.Level; i < level; i++ {
			rank[i] = 0
			update[i] = s.Head
			update[i].Levels[i].Span = s.Length
		}

		s.Level = level
	}

	x = newSkiplistNode(level, score, member)
	for i := int8(0); i < level; i++ {
		x.Levels[i].Forward = update[i].Levels[i].Forward
		update[i].Levels[i].Forward = x

		// update span covered by update[i] as x is inserted here
		x.Levels[i].Span = update[i].Levels[i].Span - (rank[0] - rank[i])
		update[i].Levels[i].Span = rank[0] - rank[i] + 1
	}

	// increment span for untouched levels
	for i := level; i < s.Level; i++ {
		update[i].Levels[i].Span++
	}

	if update[0] == s.Head {
		x.Backward = nil
	} else {
		x.Backward = update[0]
	}

	if x.Levels[0].Forward != nil {
		x.Levels[0].Forward.Backward = x
	} else {
		s.Tail = x
	}

	s.Length++

	return x
}

// Delete deletes the element with the specified score and member from the skiplist.
// Returns the deleted node if found, otherwise nil.
func (s *Skiplist) Delete(score float64, member string) *SkiplistNode {
	update := make([]*SkiplistNode, maxLevel)

	x := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		for x.Levels[i].Forward != nil &&
			(x.Levels[i].Forward.Score < score ||
				(x.Levels[i].Forward.Score == score && x.Levels[i].Forward.Member < member)) {
			x = x.Levels[i].Forward
		}

		update[i] = x
	}

	x = x.Levels[0].Forward
	if x != nil && score == x.Score && member == x.Member {
		s.deleteNode(x, update)
	}

	return x
}

// GetRank returns the rank of the element with the same score in the skiplist.
// Returns 0 if the element is not found, rand otherwise.
// Note that the rank is 1-based due to the span of s.Head to the first element.
func (s *Skiplist) GetRank(score float64, member string) int64 {
	rank := int64(0)

	x := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		for x.Levels[i].Forward != nil &&
			(x.Levels[i].Forward.Score < score ||
				(x.Levels[i].Forward.Score == score && x.Levels[i].Forward.Member <= member)) {
			rank += x.Levels[i].Span
			x = x.Levels[i].Forward
		}

		if member == x.Member {
			return rank
		}
	}

	return 0
}

// GetElementByRank returns the element at the given rank. The rank is 1-based.
func (s *Skiplist) GetElementByRank(rank int64) *SkiplistNode {
	var traversed int64

	x := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		for x.Levels[i].Forward != nil && (traversed+x.Levels[i].Span) <= rank {
			traversed += x.Levels[i].Span
			x = x.Levels[i].Forward
		}

		if traversed == rank {
			return x
		}
	}

	return nil
}

func (s *Skiplist) deleteNode(x *SkiplistNode, update []*SkiplistNode) {
	for i := int8(0); i < s.Level; i++ {
		if update[i].Levels[i].Forward == x {
			update[i].Levels[i].Span += x.Levels[i].Span - 1
			update[i].Levels[i].Forward = x.Levels[i].Forward // update forward pointer
		} else { // x is not in the update[i] level
			update[i].Levels[i].Span--
		}
	}

	if x.Levels[0].Forward != nil {
		x.Levels[0].Forward.Backward = x.Backward // update backward pointer
	} else {
		s.Tail = x.Backward
	}

	for s.Level > 1 && s.Head.Levels[s.Level-1].Forward == nil {
		s.Level--
	}

	s.Length--
}

func randomLevel() int8 {
	level := int8(1)
	for float64(rand.Int31()&0xFFFF) < (probiablity * 0xFFFF) {
		level++
	}

	if level < maxLevel {
		return level
	}

	return maxLevel
}

func newSkiplistNode(level int8, score float64, member string) *SkiplistNode {
	return &SkiplistNode{
		Member:   member,
		Score:    score,
		Backward: nil,
		Levels: make([]struct {
			Forward *SkiplistNode
			Span    int64
		}, level),
	}
}
