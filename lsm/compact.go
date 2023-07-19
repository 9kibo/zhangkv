package lsm

import (
	"sync"
)

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus //各个层级的压缩状态
	tables map[uint64]struct{}   //处于压缩状态的SSTable
}

type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

type keyRange struct {
	left  []byte
	right []byte
	inf   bool //标记，表示该keyRange的范围无限大
	size  int64
}

func (lsm *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < lsm.options.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}
