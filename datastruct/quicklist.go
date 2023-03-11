package datastruct

import (
	"container/list"
)

const pageSize = 1024

type quicklistPage []any

func (p quicklistPage) Len() int {
	return len(p)
}

type Quicklist struct {
	data   *list.List // every node is a page which is a slice of interface{}
	length int
}

type QuicklistIterator struct {
	node      *list.Element
	offset    int // offset in the page
	quicklist *Quicklist
}

func NewQuicklistIterator(q *Quicklist) *QuicklistIterator {
	return &QuicklistIterator{
		node:      nil,
		offset:    0,
		quicklist: q,
	}
}

func (iter *QuicklistIterator) Next() any {
	if iter.quicklist.length == 0 {
		return nil
	}

	if iter.node == nil {
		iter.node = iter.quicklist.data.Front()

		return iter.value()
	}

	if iter.offset++; iter.offset >= iter.page().Len() {
		if iter.node = iter.node.Next(); iter.node == nil {
			return nil
		}

		iter.offset = 0
	}

	return iter.value()
}

func (iter *QuicklistIterator) Prev() any {
	if iter.offset--; iter.offset < 0 {
		if iter.node = iter.node.Prev(); iter.node == nil {
			return nil
		}

		iter.offset = iter.page().Len() - 1
	}

	return iter.value()
}

func (iter *QuicklistIterator) page() quicklistPage {
	return iter.node.Value.(quicklistPage)
}

func (iter *QuicklistIterator) value() any {
	return iter.page()[iter.offset]
}

func NewQuicklist() *Quicklist {
	return &Quicklist{
		data:   list.New(),
		length: 0,
	}
}

func (q *Quicklist) PushBack(v any) {
	q.length++

	backNode := q.data.Back()
	if q.data.Len() == 0 || len(backNode.Value.(quicklistPage)) == pageSize {
		page := make(quicklistPage, 0, pageSize)
		page = append(page, v)
		q.data.PushBack(page)
		return
	}

	lastPage := backNode.Value.(quicklistPage)
	lastPage = append(lastPage, v)
	backNode.Value = lastPage
}

func (q *Quicklist) PushFront(v any) {
	_ = q.Insert(0, v)
}

func (q *Quicklist) PopFront() any {
	return q.Remove(0)
}

func (q *Quicklist) PopBack() any {
	return q.Remove(q.length - 1)
}

func (q *Quicklist) Insert(index int, v any) bool {
	if index < 0 || index > q.length {
		return false
	}

	if index == q.length {
		q.PushBack(v)
		return true
	}

	q.length++

	iter := q.get(index)
	page := iter.page()
	// if the page is not full, just insert the value
	if len(page) < pageSize {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = v
		iter.node.Value = page
		return true
	}

	// if the page is full, split it into two pages
	nextPage := make(quicklistPage, 0, pageSize)
	// copy the second half of the page to the next new page
	nextPage = append(nextPage, page[pageSize/2:]...)
	page = page[:pageSize/2]
	if iter.offset < len(page) {
		// if the index is in the first half of the page, insert the value into the first page
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = v
	} else {
		// if the index is in the second half of the page, insert the value into the second page
		nextPage = append(nextPage[:iter.offset-pageSize/2+1], nextPage[iter.offset-pageSize/2:]...)
		nextPage[iter.offset-pageSize/2] = v
	}

	// insert the new page into the list
	q.data.InsertAfter(nextPage, iter.node)
	iter.node.Value = page

	return true
}

func (q *Quicklist) Remove(index int) any {
	iter := q.get(index)
	if iter == nil {
		return nil
	}

	page := iter.page()
	v := page[iter.offset]

	q.length--
	page = append(page[:iter.offset], page[iter.offset+1:]...)
	if len(page) > 0 {
		iter.node.Value = page
	} else {
		q.data.Remove(iter.node)
	}

	return v
}

func (q *Quicklist) Find(v any) int {
	pageIndex := 0
	for node := q.data.Front(); node != nil; node = node.Next() {
		page := node.Value.(quicklistPage)
		for i, value := range page {
			if value == v {
				return pageIndex + i
			}
		}

		pageIndex += len(page)
	}

	return -1
}

func (q *Quicklist) Range(start, stop int) []any {
	if start < 0 {
		start = q.length + start
	}

	if stop < 0 {
		stop = q.length + stop
	}

	if start < 0 || stop < 0 || start > stop || start >= q.length {
		return nil
	}

	iter := q.get(start)
	if iter == nil {
		return nil
	}

	values := make([]any, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		values = append(values, iter.value())

		if iter.Next() == nil {
			break
		}
	}

	return values
}

func (q *Quicklist) Get(index int) any {
	iter := q.get(index)
	if iter == nil {
		return nil
	}

	return iter.value()
}

func (q *Quicklist) Len() int {
	return q.length
}

func (q *Quicklist) Size() int {
	return q.data.Len()
}

func (q *Quicklist) get(index int) *QuicklistIterator {
	if index < 0 || index >= q.length {
		return nil
	}

	pageIndex := 0

	var node *list.Element
	if index < q.length/2 {
		for node = q.data.Front(); node != nil; node = node.Next() {
			pageLen := node.Value.(quicklistPage).Len()
			if pageIndex+pageLen > index {
				break
			}

			pageIndex += pageLen
		}
	} else {
		pageIndex := q.length
		for node = q.data.Back(); node != nil; node = node.Prev() {
			pageLen := node.Value.(quicklistPage).Len()
			if pageIndex -= pageLen; pageIndex <= index {
				break
			}
		}
	}

	return &QuicklistIterator{
		node:      node,
		offset:    index - pageIndex,
		quicklist: q,
	}
}
