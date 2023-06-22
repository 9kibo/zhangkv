package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	offsetSize  = int(unsafe.Sizeof(uint32(0)))
	nodeAlign   = int(unsafe.Sizeof(uint64(0))) - 1
	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

type Arena struct {
	n   uint32
	buf []byte
}

// newArena 创建新内存池
func newArena(n int64) *Arena {
	//内存分配从1开始，0作为标记位
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}

// allocate 分配内存空间
func (s *Arena) allocate(sz uint32) uint32 {
	//原子操作
	offset := atomic.AddUint32(&s.n, sz)
	if int(offset) > len(s.buf)-MaxNodeSize {
		//计算内存池剩余大小，如果 可能 放不下下一个节点，对内存池进行扩容
		growBy := uint32(len(s.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(s.buf)+int(growBy))
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - sz
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}
func (s *Arena) putNode(height int) uint32 {
	//跳表的层级默认是按照最高层级进行分配的，但是实际高度大部分情况下是小于分配高度的
	unusedSize := (maxHeight - height) * offsetSize
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}
func (s *Arena) putVal(v ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l)
	v.EncodedValue(s.buf[offset:])
	return offset
}
func (s *Arena) putKey(key []byte) uint32 {
	keysize := uint32(len(key))
	offset := s.allocate(keysize)
	buf := s.buf[offset : offset+keysize]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}
func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0 //返回空指针
	}
	//获取某个节点,在 arena 当中的偏移量
	//unsafe.Pointer等价于void*,uintptr可以专门把void*的对于地址转化为数值型变量
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}
