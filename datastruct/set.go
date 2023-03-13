package datastruct

type Set struct {
	dict *Dict
}

func NewSet(dictType DictType) *Set {
	return &Set{dict: NewDict(dictType)}
}

func (s *Set) Add(member any) {
	s.dict.Set(member, nil)
}

func (s *Set) Delete(member any) error {
	return s.dict.Delete(member)
}

func (s *Set) Size() int64 {
	return s.dict.Size()
}

func (s *Set) Range() []any {
	iter := NewDictIterator(s.dict)
	defer iter.Release()

	keys := make([]any, 0, s.Size())
	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		keys = append(keys, entry.Key)
	}

	return keys
}

func (s *Set) GetRandom() any {
	return s.dict.GetRandomKey().Key
}

func (s *Set) Contains(member any) bool {
	return s.dict.Find(member) != nil
}

func (s *Set) Union(other *Set) *Set {
	iter1 := NewDictIterator(s.dict)
	defer iter1.Release()

	result := NewSet(s.dict.DictType)
	for entry := iter1.Next(); entry != nil; entry = iter1.Next() {
		result.Add(entry.Key.(string))
	}

	iter2 := NewDictIterator(other.dict)
	defer iter2.Release()

	for entry := iter2.Next(); entry != nil; entry = iter2.Next() {
		result.Add(entry.Key.(string))
	}

	return result
}

func (s *Set) Intersect(other *Set) *Set {
	result := NewSet(s.dict.DictType)
	iter := NewDictIterator(s.dict)
	defer iter.Release()

	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		if other.Contains(entry.Key) {
			result.Add(entry.Key.(string))
		}
	}

	return result
}

func (s *Set) Difference(other *Set) *Set {
	iter := NewDictIterator(s.dict)
	defer iter.Release()

	result := NewSet(s.dict.DictType)
	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		if !other.Contains(entry.Key) {
			result.Add(entry.Key.(string))
		}
	}

	return result
}
