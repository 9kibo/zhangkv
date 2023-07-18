package lsm

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"zhangkv/file"
	"zhangkv/utils"
)

type memTable struct {
	lsm        *LSM
	wal        *file.WalFile
	sl         *utils.Skiplist
	buf        *bytes.Buffer
	maxVersion uint64
}

const walFileExt string = ".wal"

func (lsm *LSM) NewMemtable() *memTable {
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	fileOpt := &file.Options{
		Dir:      lsm.options.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.options.MemTableSize),
		FID:      newFid,
		FileName: mtFilePath(lsm.options.WorkDir, newFid),
	}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkiplist(int64(1 << 20)), lsm: lsm}
}
func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}
func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	return nil
}

func (m *memTable) set(entry *utils.Entry) error {
	if err := m.wal.Write(entry); err != nil {
		return nil
	}
	m.sl.Add(entry)
	return nil
}
func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	// 索引检查当前的key是否在表中 O(1) 的时间复杂度
	// 从内存表中获取数据
	vs := m.sl.Search(key)

	e := &utils.Entry{
		Key:       key,
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
	}

	return e, nil

}

// recovery
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	// 从 工作目录中获取所有文件
	files, err := ioutil.ReadDir(lsm.options.WorkDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}
	var fids []uint64
	maxFid := lsm.levels.maxFID
	// 识别 后缀为.wal的文件
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), walFileExt) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-len(walFileExt)], 10, 64)
		// 考虑 wal文件的存在 更新maxFid
		if maxFid < fid {
			maxFid = fid
		}
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}
	// 排序一下子
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	imms := []*memTable{}
	// 遍历fid 做处理
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.sl.MemSize() == 0 {
			// mt.DecrRef()
			continue
		}
		imms = append(imms, mt)
	}
	// 更新最终的maxfid，初始化一定是串行执行的，因此不需要原子操作
	lsm.levels.maxFID = maxFid
	return lsm.NewMemtable(), imms
}

// 根据wal文件重放MemTable
func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	fileOpt := &file.Options{
		Dir:      lsm.options.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.options.MemTableSize),
		FID:      fid,
		FileName: mtFilePath(lsm.options.WorkDir, fid),
	}
	s := utils.NewSkiplist(int64(1 << 20))
	mt := &memTable{
		sl:  s,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	mt.wal = file.OpenWalFile(fileOpt)
	err := mt.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}
func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	endOff, err := m.wal.Iterate(true, 0, m.replayFunction(m.lsm.options))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}
	return m.wal.Truncate(int64(endOff))
}
func (m *memTable) replayFunction(opt *Options) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error {
		if ts := utils.ParseTs(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		m.sl.Add(e)
		return nil
	}
}
func (m *memTable) Size() int64 {
	return m.sl.MemSize()
}
