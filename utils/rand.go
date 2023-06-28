package utils

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex
)

func Int63n(n int64) int64 {
	mu.Lock()
	res := r.Int63n(n)
	mu.Unlock()
	return res
}

func RandN(n int) int {
	mu.Lock()
	res := r.Intn(n)
	mu.Unlock()
	return res
}

func Float64() float64 {
	mu.Lock()
	res := r.Float64()
	mu.Unlock()
	return res
}

// 生成随机字符串作为key和value
func randStr(length int) string {
	// 包括特殊字符,进行测试
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~=+%^*/()[]{}/!@#$?|©®"
	bytes := []byte(str)
	var result []byte
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

// 构建entry对象
func BuildEntry() *Entry {
	rand.Seed(time.Now().Unix())
	key := []byte(fmt.Sprintf("%s%s", randStr(16), "12345678"))
	value := []byte(randStr(128))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}
