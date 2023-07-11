package lsm

import (
	zhangCache "zhangkv/utils/cache"
)

type cache struct {
	indexs *zhangCache.Cache // key fid， value table
	blocks *zhangCache.Cache // key fid_blockOffset  value block []byte
}
type blockBuffer struct {
	b []byte
}

const defaultCacheSize = 1024

// close
func (c *cache) close() error {
	return nil
}

// newCache
func newCache(opt *Options) *cache {
	return &cache{indexs: zhangCache.NewCache(defaultCacheSize), blocks: zhangCache.NewCache(defaultCacheSize)}
}

func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
