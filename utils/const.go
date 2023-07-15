package utils

import (
	"hash/crc32"
	"math"
	"os"
)

const (
	// MaxLevelNum _
	MaxLevelNum = 7
	// DefaultValueThreshold _
	DefaultValueThreshold = 1024
)

// file
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
	MaxValueLogSize                   = 10 << 20
	datasyncFileFlag                  = 0x0
	// 基于可变长编码,其最可能的编码
	MaxHeaderSize            = 21
	VlogHeaderSize           = 0
	MaxVlogFileSize   uint32 = math.MaxUint32
	Mi                int64  = 1 << 20
	KVWriteChCapacity        = 1000
)

// meta
const (
	BitDelete       byte = 1 << 0 // 已被删除
	BitValuePointer byte = 1 << 1 // kv分离
)

// codec
var (
	MagicText          = [4]byte{'Z', 'H', 'K', 'V'}
	MagicVersion       = uint32(1)
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
