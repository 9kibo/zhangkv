package lsm

import (
	"bytes"
	"fmt"
	"sort"
	"zhangkv/utils"
)

type Iterator struct {
	it    Item
	iters []utils.Iterator
}

type Item struct {
	e *utils.Entry
}

func (it Item) Entry() *utils.Entry {
	return it.e
}

// 创建迭代器
func (lsm *LSM) NewIterators(opt *utils.Options) []utils.Iterator {
	iter := &Iterator{}
	iter.iters = make([]utils.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memTable.NewIterator(opt))
	for _, imm := range lsm.immutables {
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, lsm.levels.iterators()...)
	return iter.iters
}
func (iter *Iterator) Next() {
	iter.iters[0].Next()
}
func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}
func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}
func (iter *Iterator) Item() utils.Item {
	return iter.iters[0].Item()
}
func (iter *Iterator) Close() error {
	return nil
}

func (iter *Iterator) Seek(key []byte) {
}

// 内存表迭代器
type memIterator struct {
	innerIter utils.Iterator
}

func (m *memTable) NewIterator(opt *utils.Options) utils.Iterator {
	return &memIterator{innerIter: m.sl.NewSkipListIterator()}
}
func (iter *memIterator) Next() {
	iter.innerIter.Next()
}
func (iter *memIterator) Valid() bool {
	return iter.innerIter.Valid()
}
func (iter *memIterator) Rewind() {
	iter.innerIter.Rewind()
}
func (iter *memIterator) Item() utils.Item {
	return iter.innerIter.Item()
}
func (iter *memIterator) Close() error {
	return iter.innerIter.Close()
}
func (iter *memIterator) Seek(key []byte) {
	iter.innerIter.Seek(key)
}

// levelManager上的迭代器
type levelIterator struct {
	it    *utils.Item
	iters []*Iterator
}

func (lm *levelManager) NewIterators(options *utils.Options) []utils.Iterator {
	return lm.iterators()
}
func (iter *levelIterator) Next() {
}
func (iter *levelIterator) Valid() bool {
	return false
}
func (iter *levelIterator) Rewind() {

}
func (iter *levelIterator) Item() utils.Item {
	return &Item{}
}
func (iter *levelIterator) Close() error {
	return nil
}

func (iter *levelIterator) Seek(key []byte) {
}

// ConcatIterator 将table 数组链接成一个迭代器，这样迭代效率更高
type ConcatIterator struct {
	idx     int
	cur     utils.Iterator
	iters   []utils.Iterator
	tables  []*table
	options *utils.Options
}

func NewConcatIterator(tbls []*table, opt *utils.Options) *ConcatIterator {
	iters := make([]utils.Iterator, len(tbls))
	return &ConcatIterator{
		options: opt,
		iters:   iters,
		tables:  tbls,
		idx:     -1,
	}
}

func (s *ConcatIterator) setIdx(idx int) {
	s.idx = idx
	if idx < 0 || idx >= len(s.iters) {
		s.cur = nil
		return
	}
	if s.iters[idx] == nil {
		s.iters[idx] = s.tables[idx].NewIterator(s.options)
	}
	s.cur = s.iters[s.idx]
}
func (s *ConcatIterator) Rewind() {
	if len(s.iters) == 0 {
		return
	}
	if !s.options.IsAsc {
		s.setIdx(0)
	} else {
		s.setIdx(len(s.iters) - 1)
	}
	s.cur.Rewind()
}
func (s *ConcatIterator) Valid() bool {
	return s.cur != nil && s.cur.Valid()
}
func (s *ConcatIterator) Item() utils.Item {
	return s.cur.Item()
}
func (s *ConcatIterator) Seek(key []byte) {
	var idx int
	if s.options.IsAsc {
		idx = sort.Search(len(s.tables), func(i int) bool {
			return utils.CompareKeys(s.tables[i].ss.MaxKey(), key) >= 0
		})
	} else {
		n := len(s.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return utils.CompareKeys(s.tables[n-1-i].ss.MinKey(), key) <= 0
		})
	}
	if idx >= len(s.tables) || idx < 0 {
		s.setIdx(-1)
		return
	}
	s.setIdx(idx)
	s.cur.Seek(key)
}
func (s *ConcatIterator) Next() {
	s.cur.Next()
	if s.cur.Valid() {
		return
	}
	for { //防止有空表
		if !s.options.IsAsc {
			s.setIdx(s.idx + 1)
		} else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			return
		}
		s.cur.Rewind()
		if s.cur.Valid() {
			break
		}
	}
}
func (s *ConcatIterator) Close() error {
	for _, it := range s.iters {
		if it == nil {
			continue
		}
		if err := it.Close(); err != nil {
			return fmt.Errorf("ConcatIterator:%+v", err)
		}
	}
	return nil
}

// MergeIterator 多路合并迭代器
type MergeIterator struct {
	left  node
	right node
	small *node

	curKey  []byte
	reverse bool
}

type node struct {
	valid bool
	entry *utils.Entry
	iter  utils.Iterator

	merge  *MergeIterator
	concat *ConcatIterator
}

func (n *node) setIterator(iter utils.Iterator) {
	n.iter = iter
	n.merge, _ = iter.(*MergeIterator)
	n.concat, _ = iter.(*ConcatIterator)
}
func (n *node) setKey() {
	switch {
	case n.merge != nil:
		n.valid = n.merge.small.valid
		if n.valid {
			n.entry = n.merge.small.entry
		}
	case n.concat != nil:
		n.valid = n.concat.Valid()
		if n.valid {
			n.entry = n.concat.Item().Entry()
		}
	default:
		n.valid = n.iter.Valid()
		if n.valid {
			n.entry = n.iter.Item().Entry()
		}
	}
}

func (n *node) next() {
	switch {
	case n.merge != nil:
		n.merge.Next()
	case n.concat != nil:
		n.concat.Next()
	default:
		n.iter.Next()
	}
	n.setKey()
}

func (n *node) rewind() {
	n.iter.Rewind()
	n.setKey()
}

func (n *node) seek(key []byte) {
	n.iter.Seek(key)
	n.setKey()
}

func (mi *MergeIterator) fix() {
	if !mi.bigger().valid {
		return
	}
	if !mi.small.valid {
		mi.swapSmall()
		return
	}
	cmp := utils.CompareKeys(mi.small.entry.Key, mi.bigger().entry.Key)
	switch {
	case cmp == 0:
		mi.right.next()
		if &mi.right == mi.small {
			mi.swapSmall()
		}
		return
	case cmp < 0:
		if mi.reverse {
			mi.swapSmall()
		} else {
		}
		return
	default:
		if mi.reverse {
		} else {
			mi.swapSmall()
		}
		return
	}
}

func (mi *MergeIterator) bigger() *node {
	if mi.small == &mi.left {
		return &mi.right
	}
	return &mi.left
}

func (mi *MergeIterator) swapSmall() {
	if mi.small == &mi.left {
		mi.small = &mi.right
		return
	}
	if mi.small == &mi.right {
		mi.small = &mi.left
		return
	}
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mi *MergeIterator) Next() {
	for mi.Valid() {
		if !bytes.Equal(mi.small.entry.Key, mi.curKey) {
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}

func (mi *MergeIterator) setCurrent() {
	utils.CondPanic(mi.small.entry == nil && mi.small.valid == true, fmt.Errorf("mi.small.entry is nil"))
	if mi.small.valid {
		mi.curKey = append(mi.curKey[:0], mi.small.entry.Key...)
	}
}
func (mi *MergeIterator) Rewind() {
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
}

// Seek 后序遍历寻找
func (mi *MergeIterator) Seek(key []byte) {
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}

func (mi *MergeIterator) Valid() bool {
	return mi.small.valid
}

func (mi *MergeIterator) Item() utils.Item {
	return mi.small.iter.Item()
}

// Close implements Iterator.
func (mi *MergeIterator) Close() error {
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return utils.WarpErr("MergeIterator", err1)
	}
	return utils.WarpErr("MergeIterator", err2)
}

// NewMergeIterator creates a merge iterator.
func NewMergeIterator(iters []utils.Iterator, reverse bool) utils.Iterator {
	switch len(iters) {
	case 0:
		return &Iterator{}
	case 1:
		return iters[0]
	case 2:
		mi := &MergeIterator{
			reverse: reverse,
		}
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		// Assign left iterator randomly. This will be fixed when user calls rewind/seek.
		mi.small = &mi.left
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator(
		[]utils.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}
