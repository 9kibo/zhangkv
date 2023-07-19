package lsm

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"zhangkv/file"
	"zhangkv/utils"
)

type levelManager struct {
	maxFID       uint64 // 已经分配出去的最大fid，只要创建了memtable 就算已分配
	opt          *Options
	cache        *cache
	manifestFile *file.ManifestFile
	levels       []*levelHandler
	lsm          *LSM
	compactState *compactStatus //保存全局的压缩状态
}
type levelHandler struct {
	sync.RWMutex
	levelNum       int
	tables         []*table
	totalSize      int64
	totalStaleSize int64
	lm             *levelManager
}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}
func (lh *levelHandler) close() error {
	for i := range lh.tables {
		if err := lh.tables[i].ss.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{lsm: lsm} //反引用
	lm.compactState = lsm.newCompactStatus()
	lm.opt = opt
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm
}

func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{Dir: lm.opt.WorkDir})
	return err
}

func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			lm:       lm,
		})
	}
	manifest := lm.manifestFile.GetManifest()
	// 对比manifest 文件的正确性
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}
	//加载sst的索引，构建缓存
	lm.cache = newCache(lm.opt)
	var maxFID uint64
	for fID, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fID)
		if fID > maxFID {
			maxFID = fID
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].add(t)
		lm.levels[tableInfo.Level].addSize(t) // 记录一个level的文件总大小
	}
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

// 向L0层flush一个sstable
func (lm *levelManager) flush(immutable *memTable) (err error) {
	// 分配一个fid
	fid := immutable.wal.Fid()
	//分配一个sstable文件的路径
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)

	// 构建一个 builder
	builder := newTableBuiler(lm.opt)
	//新建一个跳表迭代器
	iter := immutable.sl.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}
	// 创建一个 table 对象
	table := openTable(lm, sstName, builder)
	err = lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       fid,
		Checksum: []byte{'m', 'o', 'c', 'k'},
	})
	// manifest写入失败直接panic
	utils.Panic(err)
	// 更新manifest文件
	lm.levels[0].add(table)
	return
}
func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}
func (lh *levelHandler) addBatch(ts []*table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, ts...)
}
func (lh *levelHandler) addSize(t *table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
}
func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		//l0层因为存在key范围重叠 根据fid从小到大排序
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].ss.MinKey(), lh.tables[j].ss.MinKey()) < 0
		})
	}
}
func (lm *levelManager) Get(key []byte) (*utils.Entry, error) {
	var (
		entry *utils.Entry
		err   error
	)
	// L0层查询
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// L1-7层查询
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err = ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, utils.ErrKeyNotFound
}
func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	// 如果是第0层文件则进行特殊处理
	if lh.levelNum == 0 {
		// 获取可能存在key的sst
		return lh.searchL0SST(key)
	} else {
		return lh.searchLNSST(key)
	}
}
func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Serach(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) searchLNSST(key []byte) (*utils.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if table == nil {
		return nil, utils.ErrKeyNotFound
	}
	if entry, err := table.Serach(key, &version); err == nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) getTable(key []byte) *table {
	if len(lh.tables) > 0 && (bytes.Compare(key, lh.tables[0].ss.MinKey()) < 0 || bytes.Compare(key, lh.tables[len(lh.tables)-1].ss.MaxKey()) > 0) {
		return nil
	} else {
		for i := len(lh.tables) - 1; i >= 0; i-- {
			if bytes.Compare(key, lh.tables[i].ss.MinKey()) > -1 &&
				bytes.Compare(key, lh.tables[i].ss.MaxKey()) < 1 {
				return lh.tables[i]
			}
		}
	}
	return nil
}
