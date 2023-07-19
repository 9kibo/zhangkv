package utils

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// CompareKeys 比较没有时间戳的key
func CompareKeys(key1, key2 []byte) int {
	CondPanic((len(key1) <= 8 || len(key2) <= 8), fmt.Errorf("%s,%s < 8", string(key1), string(key2)))
	//减少计算量，先比较前八位
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// VerifyChecksum 验证校验和
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}

func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}

// FID 根据路径获取其fid
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		Err(err)
		return 0
	}
	return uint64(id)

}

func SyncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}
func openDir(path string) (*os.File, error) { return os.Open(path) }

func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

// LoadIDMap 获取当前文件夹所有sst文件id
func LoadIDMap(dir string) map[uint64]struct{} {
	fileInfos, err := ioutil.ReadDir(dir)
	Err(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID := FID(info.Name())
		if fileID != 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}
