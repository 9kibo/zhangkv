package file

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
	"zhangkv/pb"
	"zhangkv/utils"
)

// SSTable 在ssTable中 key从小到大排列，同key的版本号从大到小排列
type SSTable struct {
	lock           *sync.RWMutex
	f              *MmapFile //关联的mmap
	maxKey         []byte
	minKey         []byte
	idxTable       *pb.TableIndex
	hasBloomFilter bool
	idxLen         int
	idxStart       int
	fid            uint64
	createAt       time.Time
}

func OpenSStable(opt *Options) *SSTable {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{f: omf, fid: opt.FID, lock: &sync.RWMutex{}}
}
func (ss *SSTable) Init() error {
	var ko *pb.BlockOffset
	var err error
	if ko, err = ss.initTable(); err != nil {
		return err
	}
	//获取创建时间
	stat, _ := ss.f.Fd.Stat()
	statType := stat.Sys().(*syscall.Stat_t)
	ss.createAt = time.Unix(statType.Ctim.Sec, statType.Ctim.Nsec)
	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey
	ss.maxKey = minKey
	return nil
}
func (ss *SSTable) SetMaxKey(maxKey []byte) {
	ss.maxKey = maxKey
}

// initTable 初始化SStable，将其索引读取到内存中,返回首个key，即当前sst中的最小key
func (ss *SSTable) initTable() (bo *pb.BlockOffset, err error) {
	readPos := len(ss.f.Data)
	//从最后4位中读取校验和长度 uint32
	readPos -= 4
	buf := ss.readCheckError(readPos, 4)
	checksumLen := int(utils.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}
	//根据校验和长度获取校验和
	readPos -= checksumLen
	expectedChk := ss.readCheckError(readPos, checksumLen)
	//读取索引长度
	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	ss.idxLen = int(utils.BytesToU32(buf))
	//读取索引
	readPos -= ss.idxLen
	ss.idxStart = readPos
	data := ss.readCheckError(readPos, ss.idxLen)
	//对crc校验和进行检验
	if err := utils.VerifyChecksum(data, expectedChk); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", ss.f.Fd.Name())
	}
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	ss.idxTable = indexTable
	ss.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		//返回首个block的offset
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")

}
func (ss *SSTable) read(off, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		if len(ss.f.Data[off:]) < sz {
			return nil, io.EOF
		}
		return ss.f.Data[off : off+sz], nil
	}
	//兜底机制，如果发生截断失败，此时mmap关联的长度为0,需要从磁盘中访问sst文件
	res := make([]byte, sz)
	_, err := ss.f.Fd.ReadAt(res, int64(off))
	return res, err
}
func (ss *SSTable) readCheckError(off, sz int) []byte {
	buf, err := ss.read(off, sz)
	utils.Panic(err)
	return buf
}
func (ss *SSTable) Close() error {
	return ss.f.Close()
}
func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}
func (ss *SSTable) Indexs() *pb.TableIndex {
	return ss.idxTable
}
func (ss *SSTable) FID() uint64 {
	return ss.fid
}
func (ss *SSTable) Delete() error {
	return ss.f.Delete()
}
