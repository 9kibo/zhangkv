package utils

import (
	"encoding/binary"
)

type ValueStruct struct {
	Meta      byte //元数据
	Value     []byte
	ExpiresAt uint64 //过期时间
	Version   uint64 //版本号，方便后期版本控制

}

// EncodedSize 计算编码后的长度
func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value) + 1 //meta
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sz:]
}

func (vs *ValueStruct) EncodedValue(b []byte) uint32 {
	b[0] = vs.Meta
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)
	n := copy(b[1+sz:], vs.Value)
	return uint32(1 + sz + n)
}

// sizeVarint 计算过期时间占用的内存空间，过期时间使用变长编码，需要单独计算
func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
