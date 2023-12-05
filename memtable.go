package easydb

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/bwmarrin/snowflake"
	badgerskl "github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/gofish2020/easydb/tinywal"
)

const (
	walFileExt     = ".MEM.%d"
	initialTableID = 1
)

// memtable: 内存对象
type memtable struct {
	wal    *tinywal.TinyWal // 保证内存数据的持久化
	option memtableOptions

	mu  sync.RWMutex
	skl *badgerskl.Skiplist // 内存数据（跳表）
}

type memtableOptions struct {
	sklMemSize      uint32 // skl内存表大小
	id              int    // 内存表编号
	walDir          string // 文件目录
	walCacheSize    uint64 // wal的缓存大小
	walIsSync       bool   // 是否立即刷盘
	walBytesPerSync uint64 // 多少字节刷盘
}

func openAllMemtables(options Options) ([]*memtable, error) {

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	// 获取内存编号 id
	var tableIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		var prefix int
		_, err := fmt.Sscanf(entry.Name(), "%d"+walFileExt, &prefix, &id)
		if err != nil {
			continue
		}
		tableIDs = append(tableIDs, id)
	}
	// 默认初始化一个内存id
	if len(tableIDs) == 0 {
		tableIDs = append(tableIDs, initialTableID)
	}
	// 排序
	sort.Ints(tableIDs)

	// 多个内存
	tables := make([]*memtable, len(tableIDs))

	for i, table := range tableIDs {
		table, err := openMemtable(memtableOptions{
			walDir:          options.DirPath,
			id:              table,
			sklMemSize:      options.MemtableSize,
			walIsSync:       options.Sync,
			walBytesPerSync: uint64(options.BytesPerSync),
		})
		if err != nil {
			return nil, err
		}
		tables[i] = table
	}

	return tables, nil
}

// 通过wal构建skl
func openMemtable(option memtableOptions) (*memtable, error) {

	// 初始化跳表
	skl := badgerskl.NewSkiplist(int64(float64(option.sklMemSize) * 1.5))

	// 初始化内存对象
	memTable := &memtable{option: option, skl: skl}
	// 构建wal对象
	wal, err := tinywal.Open(tinywal.Options{
		Dir:            option.walDir,
		SegmentFileExt: fmt.Sprintf(walFileExt, option.id), // 一个内存表对应一个文件
		SegmentSize:    math.MaxInt64,                      // 无限大
		LocalCacheSize: option.walCacheSize,
		IsSync:         option.walIsSync,
		BytesPerSync:   option.walBytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	memTable.wal = wal

	indexRecords := make(map[uint64][]*LogRecord)
	// 遍历wal中的文件，构建skl内存数据
	reader := wal.NewReader()
	for {
		data, _, err := reader.Next()
		if err != nil {
			if err == io.EOF { // 说明所有文件都读取完了
				break
			}
			// 说明出错了
			return nil, err
		}
		
		log := NewLogRecord()
		log.Decode(data)

		// batch data save to skiplist
		if log.Type == LogRecordBatchEnd {
			batchId, err := snowflake.ParseBytes(log.Key)
			if err != nil {
				return nil, err
			}

			for _, idxRecord := range indexRecords[uint64(batchId)] {
				memTable.skl.Put(y.KeyWithTs(idxRecord.Key, 0),
					y.ValueStruct{Value: idxRecord.Value, Meta: idxRecord.Type})
			}
			delete(indexRecords, uint64(batchId))
		} else {
			indexRecords[log.BatchId] = append(indexRecords[log.BatchId], log)
		}
	}

	return memTable, nil
}

// 从skl中读取数据
func (mt *memtable) get(key []byte) (bool, []byte) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	valueStruct := mt.skl.Get(y.KeyWithTs(key, 0))
	deleted := valueStruct.Meta == LogRecordDeleted
	return deleted, valueStruct.Value
}

func (mt *memtable) isFull() bool {
	return mt.skl.MemSize() >= int64(mt.option.sklMemSize)
}

func (mt *memtable) putBatch(pendingWrites map[string]*LogRecord,
	batchId snowflake.ID, options *WriteOptions) error {

	// if wal is not disabled, write to wal first to ensure durability and atomicity
	if options == nil || !options.DisableWal {
		// add record to wal.pendingWrites
		for _, record := range pendingWrites {
			record.BatchId = uint64(batchId)
			if err := mt.wal.PendingWrites(record.Encode()); err != nil {
				return err
			}
		}

		// add a record to indicate the end of the batch
		log := NewLogRecord()
		log.Key = batchId.Bytes()
		log.Type = LogRecordBatchEnd

		if err := mt.wal.PendingWrites(log.Encode()); err != nil {
			return err
		}

		// write wal.pendingWrites
		if _, err := mt.wal.WriteAll(); err != nil {
			return err
		}
		// flush wal if necessary
		if options.Sync && !mt.option.walIsSync {
			if err := mt.wal.Sync(); err != nil {
				return err
			}
		}
	}

	mt.mu.Lock()
	// write to in-memory skip list
	for key, record := range pendingWrites {
		mt.skl.Put(y.KeyWithTs([]byte(key), 0), y.ValueStruct{Value: record.Value, Meta: record.Type})
	}
	mt.mu.Unlock()

	return nil
}

func (mt *memtable) close() error {
	if mt.wal != nil {
		return mt.wal.Close()
	}
	return nil
}
