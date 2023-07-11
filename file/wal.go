package file

import (
	"bytes"
	"sync"
)

// WalFile 预写入日志
type WalFile struct {
	lock    *sync.RWMutex
	f       *MmapFile
	opts    *Options
	buf     *bytes.Buffer
	size    uint32
	writeAt uint32
}
