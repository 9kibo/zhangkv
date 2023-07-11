//go:build linux

package mmap

import (
	"golang.org/x/sys/unix"
	"os"
	"unsafe"
)

// 通过linux的系统调用实现mmap，将内存空间与磁盘进行关联
func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// Msync 调用系统调用主动进行mmap同步
func Msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
func Munmap(b []byte) error {
	if len(b) == 0 || len(b) != cap(b) {
		return unix.EINVAL
	}
	_, _, errno := unix.Syscall(unix.SYS_MUNMAP, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), 0)
	if errno != 0 {
		return errno
	}
	return nil
}
