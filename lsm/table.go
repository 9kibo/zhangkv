package lsm

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"sort"
	"sync/atomic"
	"zhangkv/file"
	"zhangkv/pb"
	"zhangkv/utils"
)

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // 引用计数
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}
	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)
	//builder存在时将buf持久化到磁盘
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid}
		//builder不存在，打开一个已经存在的sst文件
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize),
		})
	}
	//引用计数
	t.IncrRef()
	//初始化sst文件，将index加载到内存中
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	itr := t.NewIterator(&utils.Options{})
	defer itr.Close()
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	t.ss.SetMaxKey(maxKey)

	return t
}

// 加载sst中指定的block
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx == %d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	//检查缓存
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}
	var ko pb.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offste id=%d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}
	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err, "read block from sstable: %d , offset : %d ,len: %d ,failed", t.ss.FID(), b.offset, ko.GetLen())
	}

	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	t.lm.cache.blocks.Set(key, b)

	return b, nil
}

// 获取block在缓存中的key
func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= maxuint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("idx >= maxuint32"))
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}
func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}
func (t *table) Delete() error {
	return t.ss.Delete()
}
func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*ko = *index.GetOffsets()[i]
	return true
}
func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}
func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// 引用为0,删除缓存
		for i := 0; i < len(t.ss.Indexs().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// ssTable迭代器
type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (it *tableIterator) Next() {
	it.err = nil
	if it.blockPos >= len(it.t.ss.Indexs().GetOffsets()) {
		it.err = io.EOF
		return
	}
	if len(it.bi.data) == 0 { //block未初始化
		block, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.bi.tableID = it.t.fid
		it.bi.blockID = it.blockPos
		it.bi.setBlock(block)
		it.bi.seekToFirst()
		it.err = it.bi.Error()
		return
	}
	it.bi.Next()
	if !it.bi.Valid() {
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.it = it.bi.it
}

func (it *tableIterator) Valid() bool {
	return it.err != nil
}

func (it *tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}

func (it *tableIterator) Item() utils.Item {
	return it.it
}

func (it *tableIterator) Close() error {
	it.bi.Close()
	return it.t.DecrRef()
}

// Seek
// 二分法搜索 offsets
// 如果idx == 0 说明key只能在第一个block中 block[0].MinKey <= key
// 否则 block[0].MinKey > key
// 如果在 idx-1 的block中未找到key 那才可能在 idx 中
// 如果都没有，则当前key不再此table
func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(it.t.ss.Indexs().GetOffsets()) {
			return true
		}
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
}
func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}
func (it *tableIterator) seekToFirst() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToFirst()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}
func (it *tableIterator) seekToLast() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = numBlocks - 1
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToLast()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}
