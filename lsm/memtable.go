package lsm

import (
	"bytes"
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
