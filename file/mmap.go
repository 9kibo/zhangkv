package file

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"zhangkv/utils/mmap"
)

// MmapFile 利用linux的mmap将磁盘文件映射在内存中
type MmapFile struct {
	Data []byte
	Fd   *os.File
}

func OpenMmapFileUsing(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file : %s", filename)
	}

	var nerr error
	fileSize := fi.Size()
	if sz > 0 && fileSize == 0 {
		//对于一个刚创建的文件而言size一定为0，在进行内存关联的时候需要对文件进行填充，使用Truncate进行截断
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		fileSize = int64(sz)
	}
	buf, err := mmap.Mmap(fd, writable, fileSize)
	if err != nil {
		return nil, errors.Wrapf(err, "while mmapping %s with size: %d", fd.Name(), fileSize)
	}
	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		//当filesize==0可能是因为延迟写的特性，导致未同步，需要主动调用系统调用进行刷盘
		go SyncDir(dir)
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, nerr

}

// SyncDir 系统调用进行刷盘
func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}

func OpenMmapFile(filename string, flage int, maxSz int) (*MmapFile, error) {
	//打开文件获得文件的句柄
	fd, err := os.OpenFile(filename, flage, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open:%s", filename)
	}
	writable := true
	if flage == os.O_RDONLY {
		writable = false
	}
	//如果sst文件已经被创建，则使用其原来的大小
	if fileInfo, err := fd.Stat(); err == nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSz = int(fileInfo.Size())
	}
	return OpenMmapFileUsing(fd, maxSz, writable)
}

type mmapReader struct {
	Data   []byte
	offset int
}

func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MmapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   m.Data,
		offset: offset,
	}
}

// Bytes 返回指定offset后指定长度的数据
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

func (m *MmapFile) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(m.Data[offset:])
	start := offset + 4
	next := start + int(sz)
	if next > len(m.Data) {
		return []byte{}
	}
	res := m.Data[start:next]
	return res
}
func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}
func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("sync file: %s ,error: %v\n", m.Fd.Name(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("mummap file: %s ,error: %v\n", m.Fd.Name(), err)
	}
	return m.Fd.Close()

}
