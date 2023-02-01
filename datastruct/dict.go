package datastruct

import (
	"log"
	"math"
	"math/rand"

	"github.com/pkg/errors"
)

const (
	tableInitialSize = 4
	forceResizeRatio = 1
	resizeRatio      = 2
	rehashStepCount  = 1
)

var (
	ErrNotInitialized = errors.New("dict not initialized")
	ErrKeyNotFound    = errors.New("key not found")
)

type DictType interface {
	Hash(a *Object) int64
	Equal(a, b *Object) bool
}

type Entry struct {
	Key   *Object
	Value *Object
	next  *Entry
}

type hashTable struct {
	tables   []*Entry
	size     int64
	sizeMask int64
	used     int64
}

type Dict struct {
	DictType
	hashTables  [2]*hashTable
	rehashIndex int64
}

func NewDict(dictType DictType) *Dict {
	return &Dict{
		DictType:    dictType,
		rehashIndex: -1,
	}
}

func (d *Dict) Set(key, value *Object) {
	if d.isRehashing() {
		d.rehashStep()
	}

	index := d.keyIndex(key)
	// key already exists, update the value
	if index == -1 {
		entry := d.Find(key)
		entry.Value = value

		return
	}

	hTable := d.hashTables[0]
	if d.isRehashing() {
		hTable = d.hashTables[1]
	}

	hTable.tables[index] = &Entry{
		Key:   key,
		Value: value,
		next:  hTable.tables[index],
	}
	hTable.used++
}

func (d *Dict) Get(key *Object) *Object {
	entry := d.Find(key)
	if entry == nil {
		return nil
	}

	return entry.Value
}

// GetRandomKey returns a random entry from the dict.
func (d *Dict) GetRandomKey() *Entry {
	if d.hashTables[0] == nil || d.hashTables[0].used == 0 {
		return nil
	}

	if d.isRehashing() {
		d.rehashStep()
	}

	var bucket *Entry
	bucketSize := [2]int64{int64(len(d.hashTables[0].tables)), 0}
	if d.isRehashing() {
		bucketSize[1] = int64(len(d.hashTables[1].tables))
		for bucket == nil {
			randomIndex := d.rehashIndex + rand.Int63()%(bucketSize[0]+bucketSize[1]-d.rehashIndex)
			if randomIndex >= bucketSize[0] {
				bucket = d.hashTables[1].tables[randomIndex-bucketSize[0]]
			} else {
				bucket = d.hashTables[0].tables[randomIndex]
			}
		}
	} else {
		for bucket == nil {
			randomIndex := rand.Int63() % bucketSize[0]
			bucket = d.hashTables[0].tables[randomIndex]
		}
	}

	listLength := 0
	for entry := bucket; entry != nil; entry = entry.next {
		listLength++
	}

	bucketRandomIndex := rand.Intn(listLength)
	for i := 0; i < bucketRandomIndex; i++ {
		bucket = bucket.next
	}

	return bucket
}

func (d *Dict) Delete(key *Object) error {
	if d.hashTables[0] == nil {
		return ErrNotInitialized
	}

	if d.isRehashing() {
		d.rehashStep()
	}

	for table := 0; table <= 1; table++ {
		index := d.Hash(key) & d.hashTables[table].sizeMask
		entry := d.hashTables[table].tables[index]
		var prev *Entry
		for entry != nil {
			if d.Equal(key, entry.Key) {
				if prev != nil {
					prev.next = entry.next
				} else {
					d.hashTables[table].tables[index] = entry.next
				}

				d.hashTables[table].used--

				return nil
			}

			prev, entry = entry, entry.next
		}

		if !d.isRehashing() {
			break
		}
	}

	return ErrKeyNotFound
}

func (d *Dict) Find(key *Object) *Entry {
	if d.hashTables[0] == nil {
		return nil
	}

	if d.isRehashing() {
		d.rehashStep()
	}

	for table := 0; table <= 1; table++ {
		index := d.Hash(key) & d.hashTables[table].sizeMask
		entry := d.hashTables[table].tables[index]
		for entry != nil {
			if d.Equal(key, entry.Key) {
				return entry
			}

			entry = entry.next
		}

		if !d.isRehashing() {
			break
		}
	}

	return nil
}

func (d *Dict) Size() int64 {
	if d.hashTables[0] == nil {
		return 0
	}

	if d.isRehashing() {
		return d.hashTables[0].used + d.hashTables[1].used
	}

	return d.hashTables[0].used
}

// keyIndex returns the index of a free slot that can be used to store the given key.
// if the key already exists, -1 is returned.
// Note that if it is in the process of rehashing, the index is always returned in the second (new) hash table.
func (d *Dict) keyIndex(key *Object) int64 {
	if err := d.expandIfNeeded(); err != nil {
		log.Println(err)

		return -1
	}

	var index int64
	for table := 0; table <= 1; table++ {
		index = d.Hash(key) & d.hashTables[table].sizeMask
		entry := d.hashTables[table].tables[index]
		for entry != nil {
			if d.Equal(key, entry.Key) {
				return -1
			}

			entry = entry.next
		}

		if !d.isRehashing() {
			break
		}
	}

	return index
}

func (d *Dict) expandIfNeeded() error {
	if d.isRehashing() {
		return nil
	}

	if d.hashTables[0] == nil {
		return d.expand(tableInitialSize)
	}

	if d.hashTables[0].used/d.hashTables[0].size > forceResizeRatio {
		return d.expand(d.hashTables[0].size * resizeRatio)
	}

	return nil
}

func (d *Dict) expand(size int64) error {
	realSize := GetNextPower(size)
	if d.isRehashing() || (d.hashTables[0] != nil && d.hashTables[0].size >= realSize) {
		return errors.New("dict is rehashing or size is invalid")
	}

	table := &hashTable{
		tables:   make([]*Entry, realSize),
		size:     realSize,
		sizeMask: realSize - 1,
		used:     0,
	}

	// Is this the first initialization? if so, only need to initialize the first hash table
	if d.hashTables[0] == nil {
		d.hashTables[0] = table

		return nil
	}

	// Prepare a second hash table for incremental rehashing
	d.hashTables[1] = table
	d.rehashIndex = 0

	return nil
}

func (d *Dict) rehashStep() {
	d.rehash(rehashStepCount)
}

func (d *Dict) rehash(step int) {
	for ; step > 0; step-- {
		// Check if already rehashed the whole table ...
		if d.hashTables[0].used == 0 {
			// Free the old hash table and set the new one as main
			d.hashTables[0], d.hashTables[1] = d.hashTables[1], nil
			d.rehashIndex = -1

			return
		}

		// Find non empty slot index
		for d.hashTables[0].tables[d.rehashIndex] == nil {
			d.rehashIndex++
		}

		entry := d.hashTables[0].tables[d.rehashIndex]
		// Move all the keys in this slot from the old to the new hash table
		for entry != nil {
			nextEntry := entry.next
			// Get the index in the new hash table
			index := d.Hash(entry.Key) & d.hashTables[1].sizeMask
			entry.next = d.hashTables[1].tables[index]
			d.hashTables[1].tables[index] = entry
			d.hashTables[0].used--
			d.hashTables[1].used++
			entry = nextEntry
		}

		d.hashTables[0].tables[d.rehashIndex] = nil
		d.rehashIndex++
	}
}

func (d *Dict) isRehashing() bool {
	return d.rehashIndex != -1
}

func GetNextPower(size int64) int64 {
	if size <= tableInitialSize {
		return tableInitialSize
	}

	res := size - 1
	for i := 1; i <= 64; i *= 2 {
		res |= res >> i
	}

	if res < 0 || res == math.MaxInt64 {
		return math.MaxInt64
	}

	return res + 1
}
