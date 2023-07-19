package lsm

import (
	"zhangkv/utils"
)

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	options    *Options
	closer     *utils.Closer
	maxMemFID  uint32
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

func (lsm *LSM) Close() error {
	lsm.closer.Close()
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return nil
	}
	return nil
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{options: opt}
	//初始话levelManager
	lsm.levels = lsm.initLevelManager(opt)
	// 启动DB恢复过程加载wal，如果没有恢复内容则创建新的内存表
	lsm.memTable, lsm.immutables = lsm.recovery()
	// 初始化closer 用于资源回收的信号控制
	lsm.closer = utils.NewCloser()
	return lsm
}
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 优雅关闭
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	// 检查当前memtable是否写满，是的话创建新的memtable,并将当前内存表写到immutables中
	// 否则写入当前memtable中
	if int64(lsm.memTable.wal.Size())+int64(utils.EstimateWalCodecSize(entry)) > lsm.options.MemTableSize {
		lsm.Rotate()
	}

	if err = lsm.memTable.set(entry); err != nil {
		return err
	}
	// 检查是否存在immutable需要刷盘，
	for _, immutable := range lsm.immutables {
		if err = lsm.levels.flush(immutable); err != nil {
			return err
		}
		err = immutable.close()
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}
func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemtable()
}

// Get 从lsm中检索
func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	var (
		entry *utils.Entry
		err   error
	)
	// 从内存表中查询,先查活跃表，再查不变表
	if entry, err = lsm.memTable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}
	//为防止io阻塞，immutable采用的是数组的形式，从前往后插入，在检索时需要获取最新插入的数据，所以从后往前查询
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.immutables[i].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	// 从level manger查询
	return lsm.levels.Get(key)
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memTable.Size()
}

func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memTable == nil
}

func (lsm *LSM) GetSkipListFromMemTable() *utils.Skiplist {
	return lsm.memTable.sl
}
