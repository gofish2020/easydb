package easydb

import (
	"fmt"
	"sync"

	"github.com/bwmarrin/snowflake"
)

func makeBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

type Batch struct {
	db            *DB
	pendingWrites map[string]*LogRecord
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool
	batchId       *snowflake.Node
}

// NewBatch开启读/写事物
func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:        db,
		options:   options,
		committed: false,
	}
	if !options.ReadOnly {
		batch.pendingWrites = make(map[string]*LogRecord)
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	// 开启实务（对db加锁）
	batch.lock()
	return batch
}

// 锁定db
func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

// 解锁db
func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// 保存数据到 pendingWrites中
func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// 将写操作缓存在 pendingWrites 中
	b.pendingWrites[string(key)] = &LogRecord{
		Key:   key,
		Value: value,
		Type:  LogRecordNormal,
	}
	b.mu.Unlock()

	return nil
}

// 获取数据
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}

	// 从pendWrites读取数据
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			if record.Type == LogRecordDeleted {
				b.mu.RUnlock()
				return nil, ErrKeyNotFound
			}
			b.mu.RUnlock()
			return record.Value, nil
		}
		b.mu.RUnlock()
	}

	// 遍历所有内存(倒序读取内存中保存的数据)
	tables := b.db.getMemTables()
	for _, table := range tables {
		// 从内存中skl中读取
		deleted, value := table.get(key)
		if deleted {
			return nil, ErrKeyNotFound
		}
		if len(value) != 0 {
			return value, nil
		}
	}

	return nil, ErrKeyNotFound
}

// 删除key
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	// 构建删除记录
	b.mu.Lock()
	b.pendingWrites[string(key)] = &LogRecord{
		Key:  key,
		Type: LogRecordDeleted,
	}
	b.mu.Unlock()

	return nil
}

// 批量提交
func (b *Batch) Commit(options *WriteOptions) error {
	// use the default options if options is nil
	if options == nil {
		options = &WriteOptions{Sync: false, DisableWal: false}
	}
	// 关闭实务（对db解锁）
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// check if committed
	if b.committed {
		return ErrBatchCommitted
	}

	// wait for memtable space
	if err := b.db.waitMemtableSpace(); err != nil {
		return err
	}
	batchId := b.batchId.Generate()
	// call memtable put batch
	err := b.db.activeMem.putBatch(b.pendingWrites, batchId, options)
	if err != nil {
		return err
	}

	b.committed = true
	return nil
}

func (b *Batch) init(rdonly, sync bool, db *DB) *Batch {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	b.lock()
	return b
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = nil
	b.committed = false
}

func (b *Batch) withPendingWrites() *Batch {
	b.pendingWrites = make(map[string]*LogRecord)
	return b
}
