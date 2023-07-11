package lsm

import (
	"sync"
	"zhangkv/file"
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
