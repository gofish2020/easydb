// 一个目录下有多个文件 -> 每个文件对应一个segment(activeSegment immutableSegment) -> 一个文件由多个block组成，block中保存chunk记录（实际数据由多个chunk组成）

package tinywal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

type TinyWal struct {
	//wal 配置信息
	option Options
	// 写锁
	mutex sync.RWMutex
	// 活跃segment
	activeSegmentFile *segmentFile
	// 不可变segment
	immutableSegmentFile map[SegmentFileId]*segmentFile
	// lru (key/value 内存缓存) 所有segment文件复用【key = 文件ID+blockid & value = 32kb的block】
	localCache *lru.Cache[uint64, []byte]
	// 记录已经写入的字节数
	bytesWrite uint64

	pendingWritesLock sync.Mutex
	pendingWrites     [][]byte
	pendingSize       int64
}

// Open 基于目录中的segment file构建tinywal对象
func Open(option Options) (*TinyWal, error) {

	// 判断文件后缀格式是否正确
	if !strings.HasPrefix(option.SegmentFileExt, ".") {
		return nil, errors.New("文件后缀必须以.开头")
	}

	// 如果目录不存在创建目录；如果目录存在啥也不做
	err := os.MkdirAll(option.Dir, fs.ModePerm)
	if err != nil {
		return nil, err
	}

	tinywal := TinyWal{
		option:               option,
		immutableSegmentFile: make(map[SegmentFileId]*segmentFile),
		activeSegmentFile:    nil,
	}

	// LocalCacheSize > 0 需要缓存
	if option.LocalCacheSize > 0 {
		// 计算缓存block个数
		blockNum := option.LocalCacheSize / blockSize
		if option.LocalCacheSize%blockSize != 0 {
			blockNum++
		}
		tinywal.localCache, err = lru.New[uint64, []byte](int(blockNum))
		if err != nil {
			return nil, err
		}
	}

	// 读取目录中所有文件
	dirEntries, err := os.ReadDir(option.Dir)
	if err != nil {
		return nil, err
	}

	var segmentFileIds []int
	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}
		// 提取文件名中以SegmentFileExt后缀的【前面的名字】
		segmentFileId := 0
		_, err = fmt.Sscanf(entry.Name(), "%d"+option.SegmentFileExt, &segmentFileId)
		if err != nil { // 说明这个文件名格式不符合
			continue
		}
		segmentFileIds = append(segmentFileIds, segmentFileId)
	}

	if len(segmentFileIds) == 0 { // 说明第一次
		segment, err := openSegmentFile(option.Dir, option.SegmentFileExt, firstSegmentFileId, tinywal.localCache)
		if err != nil {
			return nil, err
		}
		tinywal.activeSegmentFile = segment
	} else {

		sort.Ints(segmentFileIds)
		for i, segmentFileId := range segmentFileIds { // 打开所有的文件
			segment, err := openSegmentFile(option.Dir, option.SegmentFileExt, uint32(segmentFileId), tinywal.localCache)
			if err != nil {
				return nil, err
			}

			if i == len(segmentFileIds)-1 { // 最后一个文件为活跃segment
				tinywal.activeSegmentFile = segment
			} else {
				tinywal.immutableSegmentFile[uint32(segmentFileId)] = segment
			}
		}
	}
	return &tinywal, nil
}

func (tinywal *TinyWal) isFull(delta int64) bool {
	return tinywal.activeSegmentFile.Size()+tinywal.maxWriteSize(delta) > int64(tinywal.option.SegmentSize)
}

// 可能占用的最大空间
func (tinywal *TinyWal) maxWriteSize(delta int64) int64 {
	return chunkHeaderSize + delta + (delta/blockSize+1)*chunkHeaderSize
}

// 创建新的 activeSegmentFile 替换代替当前 activeSegmentFile
func (tinywal *TinyWal) replaceActiveSegmentFile() error {

	if err := tinywal.activeSegmentFile.Sync(); err != nil {
		return err
	}
	tinywal.bytesWrite = 0
	segmentFile, err := openSegmentFile(tinywal.option.Dir, tinywal.option.SegmentFileExt, tinywal.activeSegmentFile.segmentFileId+1, tinywal.localCache)
	if err != nil {
		return err
	}

	tinywal.immutableSegmentFile[tinywal.activeSegmentFile.segmentFileId] = tinywal.activeSegmentFile
	tinywal.activeSegmentFile = segmentFile
	return nil
}

// Write 往tinywal对象中写入字节流
func (tinywal *TinyWal) Write(data []byte) (*ChunkPosition, error) {
	tinywal.mutex.Lock()
	defer tinywal.mutex.Unlock()

	// 1.判断数据是否比文件还大
	if (tinywal.maxWriteSize(int64(len(data)))) > tinywal.option.SegmentSize {
		return nil, ErrDataTooLarge
	}
	//2. 当前文件的剩余空间，能否写入数据
	if tinywal.isFull(int64(len(data))) {
		// 新建一个文件
		if err := tinywal.replaceActiveSegmentFile(); err != nil {
			return nil, err
		}
	}
	//3. 将数据写入到 activeSegmentFile中
	pos, err := tinywal.activeSegmentFile.Write(data)
	if err != nil {
		return nil, err
	}

	// 4.记录已经写入多少字节
	tinywal.bytesWrite += uint64(pos.ChunkSize)

	isSync := tinywal.option.IsSync
	//5.是否将数据刷盘（基于配置）
	if !isSync && tinywal.bytesWrite > tinywal.option.BytesPerSync {
		isSync = true
	}
	// 6.执行刷盘
	if isSync {
		if err := tinywal.activeSegmentFile.Sync(); err != nil {
			return nil, err
		}
		tinywal.bytesWrite = 0
	}
	return pos, nil
}

// Read 从tinywal中读取数据
func (tinywal *TinyWal) Read(pos *ChunkPosition) ([]byte, error) {
	tinywal.mutex.RLock()
	defer tinywal.mutex.RUnlock()

	var segment *segmentFile
	if pos.SegmentFileId == tinywal.activeSegmentFile.segmentFileId {
		segment = tinywal.activeSegmentFile
	} else {
		if seg, ok := tinywal.immutableSegmentFile[pos.SegmentFileId]; ok {
			segment = seg
		}
	}

	if segment == nil {
		return nil, fmt.Errorf("segment file %d%s not found", pos.SegmentFileId, tinywal.option.SegmentFileExt)
	}

	// 从文件中读取数据
	return segment.Read(pos.BlockIndex, pos.ChunkOffset)
}

func (tinywal *TinyWal) NewReader() *Reader {
	tinywal.mutex.RLock()
	defer tinywal.mutex.RUnlock()

	var readers []*segmentReader
	for _, segmentFile := range tinywal.immutableSegmentFile {
		readers = append(readers, segmentFile.NewSegmentReader())
	}

	readers = append(readers, tinywal.activeSegmentFile.NewSegmentReader())

	// 基于文件编号排序
	sort.Slice(readers, func(i, j int) bool {
		return readers[i].seg.segmentFileId < readers[j].seg.segmentFileId
	})

	return &Reader{
		allSegmentReader: readers,
		progress:         0,
	}
}

func (tinywal *TinyWal) PendingWrites(data []byte) error {
	tinywal.pendingWritesLock.Lock()
	defer tinywal.pendingWritesLock.Unlock()

	size := tinywal.maxWriteSize(int64(len(data)))
	tinywal.pendingSize += size
	tinywal.pendingWrites = append(tinywal.pendingWrites, data)
	return nil
}

// 清空
func (tinywal *TinyWal) ClearPendingWrites() {
	tinywal.pendingWritesLock.Lock()
	defer tinywal.pendingWritesLock.Unlock()

	tinywal.pendingSize = 0
	tinywal.pendingWrites = tinywal.pendingWrites[:0]
}

func (tinywal *TinyWal) WriteAll() ([]*ChunkPosition, error) {
	if len(tinywal.pendingWrites) == 0 {
		return make([]*ChunkPosition, 0), nil
	}

	tinywal.mutex.Lock()
	defer func() {
		tinywal.ClearPendingWrites()
		tinywal.mutex.Unlock()
	}()

	// 数据不能超过文件大小
	if tinywal.pendingSize > tinywal.option.SegmentSize {
		return nil, ErrPendingSizeTooLarge
	}

	// 文件剩余空间不够
	if tinywal.activeSegmentFile.Size()+tinywal.pendingSize > tinywal.option.SegmentSize {
		if err := tinywal.replaceActiveSegmentFile(); err != nil {
			return nil, err
		}
	}

	// write all data to the active segment file.
	positions, err := tinywal.activeSegmentFile.writeAll(tinywal.pendingWrites)
	if err != nil {
		return nil, err
	}

	return positions, nil
}

func (tinywal *TinyWal) Sync() error {
	tinywal.mutex.Lock()
	defer tinywal.mutex.Unlock()

	return tinywal.activeSegmentFile.Sync()
}

func (wal *TinyWal) Close() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	// purge the block cache.
	if wal.localCache != nil {
		wal.localCache.Purge()
	}

	// close all segment files.
	for _, segment := range wal.immutableSegmentFile {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	wal.immutableSegmentFile = nil

	// close the active segment file.
	return wal.activeSegmentFile.Close()
}
