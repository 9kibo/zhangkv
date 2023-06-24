package utils

import (
	"bytes"
	"fmt"
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
