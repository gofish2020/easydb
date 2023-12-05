package easydb

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
)

const (
	fileLockName = "FLOCK"
)

// 用来保存kv数据，写入到activeMem，读取从所有的 mem中读取；immutableMem可以用来归档
type DB struct {
	mu           sync.RWMutex
	activeMem    *memtable   // 活跃内存
	immutableMem []*memtable // 不可变内存
	closed       bool

	batchPool sync.Pool
}

func Open(options Options) (*DB, error) {

	// 判断目录是否存在
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 目录锁
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// 打开所有内存文件，构造内存数据
	memtables, err := openAllMemtables(options)
	if err != nil {
		return nil, err
	}

	db := &DB{
		activeMem:    memtables[len(memtables)-1],
		immutableMem: memtables[:len(memtables)-1],
		batchPool:    sync.Pool{New: makeBatch},
	}
	return db, nil
}

func (db *DB) getMemTables() []*memtable {

	// 倒序的方式（也就是从最新的mem读）
	var tables []*memtable
	tables = append(tables, db.activeMem)

	last := len(db.immutableMem) - 1
	for i := range db.immutableMem {
		tables = append(tables, db.immutableMem[last-i]) // 倒序
	}
	return tables
}

func (db *DB) waitMemtableSpace() error {
	if !db.activeMem.isFull() {
		return nil
	}

	// 当前活跃，变成不可变
	db.immutableMem = append(db.immutableMem, db.activeMem)
	// 重新生成一个新的活跃内存
	options := db.activeMem.option
	options.id++
	table, err := openMemtable(options)
	if err != nil {
		return err
	}
	db.activeMem = table
	return nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// close all memtables
	for _, table := range db.immutableMem {
		if err := table.close(); err != nil {
			return err
		}
	}
	if err := db.activeMem.close(); err != nil {
		return err
	}
	db.closed = true
	return nil
}

func (db *DB) Put(key []byte, value []byte, options *WriteOptions) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single put operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db).withPendingWrites()
	if err := batch.Put(key, value); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit(options)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit(nil)
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Get(key)
}

func (db *DB) Delete(key []byte, options *WriteOptions) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single delete operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db).withPendingWrites()
	if err := batch.Delete(key); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit(options)
}
