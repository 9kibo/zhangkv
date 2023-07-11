package lsm

import (
	"os"
	"sync/atomic"
	"zhangkv/file"
	"zhangkv/utils"
)

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
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

}
func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

// ssTable迭代器
type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *block
}
