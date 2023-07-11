package file

import (
	"os"
	"sync"
)

// ManifestFile 维护sst文件元信息的文件
// manifest 比较特殊，不能使用mmap，需要保证实时的写入
type ManifestFile struct {
	opt                       *Options
	f                         *os.File
	lock                      sync.Mutex
	deletionsRewriteThreshold int //manifest文件重写阈值
	manifest                  *Manifest
}

// Manifest corekv 元数据状态维护
type Manifest struct {
	Levels    []levelManifest          //各个层级的sst文件集合
	Tables    map[uint64]TableManifest //通过文件名快速获取sst文件所在的层级
	Creations int                      //创建的操作数
	Deletions int                      //删除的操作数
}
type levelManifest struct {
	Tables map[uint64]struct{}
}

// TableManifest 包含sst的基本信息
type TableManifest struct {
	Level    uint8
	Checksum []byte // 方便今后扩展
}
