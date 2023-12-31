package easydb

import (
	"os"

	"github.com/gofish2020/easydb/tinywal"
)

type Options struct {
	// DirPath specifies the directory path where all the database files will be stored.
	DirPath string

	// MemtableSize represents the maximum size in bytes for a memtable.
	// It means that each memtable will occupy so much memory.
	// Default value is 64MB.
	MemtableSize uint32

	// BlockCache specifies the size of the block cache in number of bytes.
	// A block cache is used to store recently accessed data blocks, improving read performance.
	// If BlockCache is set to 0, no block cache will be used.
	BlockCache uint32

	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	Sync bool

	// BytesPerSync specifies the number of bytes to write before calling fsync.
	BytesPerSync uint32
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "easydb-temp")
	return dir
}

var DefaultOptions = Options{
	DirPath:      tempDBDir(),
	MemtableSize: 64 * tinywal.MB,
	BlockCache:   0,
	Sync:         false,
	BytesPerSync: 0,
}

// BatchOptions specifies the options for creating a batch.
type BatchOptions struct {
	// Sync has the same semantics as Options.Sync.
	Sync bool

	// ReadOnly specifies whether the batch is read only.
	ReadOnly bool
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

type WriteOptions struct {
	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.

	// Default value is false.
	Sync bool

	// DisableWal if true, writes will not first go to the write ahead log, and the write may get lost after a crash.
	// Setting true only if don`t care about the data loss.
	// Default value is false.
	DisableWal bool
}
