package lsm

import (
	"zhangkv/utils"
)

type LSM struct {
	memTable  *memTable
	immutable []*memTable
	levels    *levelManager
	options   *Options
	closer    *utils.Closer
	maxMemFID uint32
}

// Options _
type Options struct {
	//工作目录
	WorkDir string
	//memtable的大小，超过规定大小就需要进行刷盘
	MemTableSize int64
	//单个sstable最大容量
	SSTableMaxSz int64
	// BlockSize sstable中每个块的大小
	BlockSize int
	// BloomFalsePositive 布隆过滤器可以接受的误报率
	BloomFalsePositive float64

	// compact
	NumCompactors       int //开启的压缩协程数量
	BaseLevelSize       int64
	LevelSizeMultiplier int // 决定level之间期望的size比例
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int

	DiscardStatsCh *chan map[uint32]int64
}
func (lsm *LSM)Close() error{
	lsm.closer.Close()
	if lsm.memTable != nil{
		if err := lsm.memTable.close
	}
}