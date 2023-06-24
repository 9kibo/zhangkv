package utils

import (
	"github.com/pkg/errors"
	"log"
	"math"
	"sync/atomic"
	_ "unsafe"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

type node struct {
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value     uint64
	keyOffset uint32
	keySize   uint16
	height    uint16
	tower     [maxHeight]uint32
}
type Skiplist struct {
	height     int32
	headOffset uint32
	ref        int32
	arena      *Arena
	OnClose    func() //回调函数
}

// IncrRef 引用计数
func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	if s.OnClose != nil {
		s.OnClose()
	}
	s.arena = nil
}
func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}
func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	node := arena.getNode(nodeOffset)
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = val
	return node
}
func NewSkiplist(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, ValueStruct{}, maxHeight)
	ho := arena.getNodeOffset(head)
	return &Skiplist{
		height:     1,
		headOffset: ho,
		ref:        1,
		arena:      arena,
	}
}

func (n *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

func (n *node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}
func (n *node) setValue(value uint64) {
	atomic.StoreUint64(&n.value, value)
}
func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}
func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}
func (n *node) getVs(arena *Arena) ValueStruct {
	valOffset, valSize := n.getValueOffset()
	return arena.getVal(valOffset, valSize)
}

// FastRand go高级用法调用运行时中的随机函数
//
//go:linkname FastRand runtime.fastrand
func FastRand() uint32

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}
	return h
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}
func (s *Skiplist) getHead() *node {
	return s.arena.getNode(s.headOffset)
}
func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// 寻找给定key在跳表中最接近的点，allowEqual 是否允许相等，less = true 寻找key最左边节点
func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.getHead()
	level := int(s.getHeight() - 1)
	for {
		next := s.getNext(x, level)
		if next == nil {
			if level > 0 {
				level--
				continue
			}
			//无法返回比key大的值，返回nil
			if !less {
				return nil, false
			}
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}

		nextKey := next.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp > 0 {
			x = next
			continue
		}
		if cmp == 0 {
			//找到相等节点
			if allowEqual {
				return next, true
			}
			if !less {
				return s.getNext(next, 0), false
			}
			if level > 0 {
				level--
				continue
			}
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}
		if level > 0 {
			level--
			continue
		}
		if !less {
			return next, false
		}
		if x == s.getHead() {
			return nil, false
		}
		return x, false
	}

}

// findSpliceForLevel 寻找给定key在指定层的插入位置
func (s *Skiplist) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		beforeNode := s.arena.getNode(before)
		next := beforeNode.getNextOffset(level)
		nextNode := s.arena.getNode(next)
		if nextNode == nil {
			return before, next
		}
		nextKey := nextNode.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp == 0 {
			//需要进行值替换，返回两个相同的值，交给外层处理
			return next, next
		}
		if cmp < 0 {
			return before, next
		}
		before = next
	}
}

// Add 添加kv对
func (s *Skiplist) Add(e *Entry) {
	key, v := e.Key, ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Version:   e.Version,
	}
	listHeight := s.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32

	prev[listHeight] = s.headOffset
	for i := int(listHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			//跳表中已经存在该key，不需要添加而是修改最底层的value值即可
			vo := s.arena.putVal(v)
			encValue := encodeValue(vo, v.EncodedSize())
			prevNode := s.arena.getNode(prev[i])
			prevNode.setValue(encValue)
			return
		}
	}
	height := s.randomHeight()
	x := newNode(s.arena, key, v, height)

	listHeight = s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			// 更新跳表的高度 使用官方库实现cas
			break
		}
		listHeight = s.getHeight()
	}
	for i := 0; i < height; i++ {
		for {
			if s.arena.getNode(prev[i]) == nil {
				AssertTrue(i > 0)
				prev[i], next[i] = s.findSpliceForLevel(key, s.headOffset, i)
				AssertTrue(prev[i] != next[i])
			}
			x.tower[i] = next[i]
			pnode := s.arena.getNode(prev[i])
			if pnode.casNextOffset(i, next[i], s.arena.getNodeOffset(x)) {
				break
			}
			//在获取一遍插入位置，如果出现重复key说明发生了并发，则此时只改变底层value值即可
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				vo := s.arena.putVal(v)
				encValue := encodeValue(vo, v.EncodedSize())
				prevNode := s.arena.getNode(prev[i])
				prevNode.setValue(encValue)
				return
			}
		}
	}
}
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}

// Empty returns if the Skiplist is empty.
func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will NEVER return the head nodes.
func (s *Skiplist) findLast() *node {
	n := s.getHead()
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.getHead() {
				return nil
			}
			return n
		}
		level--
	}
}

// Search 查找节点
func (s *Skiplist) Search(key []byte) ValueStruct {
	n, _ := s.findNear(key, false, true) // 大于等于
	if n == nil {
		return ValueStruct{}
	}

	nextKey := s.arena.getKey(n.keyOffset, n.keySize)
	if !SameKey(key, nextKey) {
		return ValueStruct{}
	}

	valOffset, valSize := n.getValueOffset()
	vs := s.arena.getVal(valOffset, valSize)
	vs.ExpiresAt = ParseTs(nextKey)
	return vs
}

// NewSkipListIterator 返回一个跳表的迭代器绑定跳表
func (s *Skiplist) NewSkipListIterator() Iterator {
	s.IncrRef()
	return &SkipListIterator{list: s}
}

// MemSize returns the size of the Skiplist in terms of how much memory is used within its internal
// arena.
func (s *Skiplist) MemSize() int64 { return s.arena.size() }

// SkipListIterator 一个跳表的迭代器
type SkipListIterator struct {
	list *Skiplist
	n    *node
}

func (s *SkipListIterator) Rewind() {
	s.SeekToFirst()
}

func (s *SkipListIterator) Item() Item {
	return &Entry{
		Key:       s.Key(),
		Value:     s.Value().Value,
		ExpiresAt: s.Value().ExpiresAt,
		Meta:      s.Value().Meta,
		Version:   s.Value().Version,
	}
}

// Close 释放当前迭代器
func (s *SkipListIterator) Close() error {
	s.list.DecrRef()
	return nil
}

// Valid 返回当前节点是否有效
func (s *SkipListIterator) Valid() bool { return s.n != nil }

// Key 返回迭代器当前节点的key
func (s *SkipListIterator) Key() []byte {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

// Value 返回迭代器当前节点的value
func (s *SkipListIterator) Value() ValueStruct {
	valueOffset, valueSize := s.n.getValueOffset()
	return s.list.arena.getVal(valueOffset, valueSize)
}

// ValueUint64 返回当前节点的value的编码
func (s *SkipListIterator) ValueUint64() uint64 {
	return s.n.value
}

// Next 迭代器前进
func (s *SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

// Prev 迭代器节点移动到前驱
func (s *SkipListIterator) Prev() {
	AssertTrue(s.Valid())
	s.n, _ = s.list.findNear(s.Key(), true, false) // find <. No equality allowed.
}

// Seek 找到 >= target 的第一个节点
func (s *SkipListIterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true)
}

// SeekForPrev 找到 <= target 的第一个节点
func (s *SkipListIterator) SeekForPrev(target []byte) {
	s.n, _ = s.list.findNear(target, true, true)
}

// SeekToFirst 定位到链表的第一个节点
func (s *SkipListIterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.getHead(), 0)
}

// SeekToLast 跳转到最后一位
func (s *SkipListIterator) SeekToLast() {
	s.n = s.list.findLast()
}

// UniIterator 一个迭代器的简单封装
type UniIterator struct {
	iter     *Iterator
	reversed bool
}
