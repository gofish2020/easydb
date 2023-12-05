package tinywal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	lru "github.com/hashicorp/golang-lru/v2"
)

type SegmentFileId = uint32

type segmentFile struct {
	// 文件Id
	segmentFileId SegmentFileId
	// 文件句柄
	fd *os.File
	// 最后一个block的索引
	lastBlockIndex uint32
	// 最后一个block的大小
	lastBlockSize uint32
	// 7字节chunkheader(复用，避免内存重复分配)
	header []byte
	closed bool
	//localCache
	localCache *lru.Cache[uint64, []byte]
}

// 拼接文件名
func segmentFileName(dir, ext string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%010d"+ext, id))
}

// openSegmentFile 打开一个文件
func openSegmentFile(dir string, ext string, segmentFileId uint32, localCache *lru.Cache[uint64, []byte]) (*segmentFile, error) {

	fd, err := os.OpenFile(segmentFileName(dir, ext, segmentFileId), os.O_CREATE|os.O_RDWR|os.O_APPEND, segmentFileModePerm)
	if err != nil {
		return nil, err
	}

	fileInfo, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	size := fileInfo.Size() // 当前文件大小

	return &segmentFile{
		segmentFileId:  segmentFileId,
		fd:             fd,
		lastBlockIndex: uint32(size / blockSize),
		lastBlockSize:  uint32(size % blockSize),
		localCache:     localCache,
		header:         make([]byte, chunkHeaderSize),
	}, nil
}

// close file
func (seg *segmentFile) Close() error {
	if seg.closed {
		return nil
	}
	seg.closed = true
	return seg.fd.Close()
}

// file size
func (seg *segmentFile) Size() int64 {
	return int64(seg.lastBlockIndex*blockSize + seg.lastBlockSize)
}

// 文件刷盘
func (seg *segmentFile) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

// 追加chunk到buf中
func (seg *segmentFile) appendChunk2Buffer(buf *bytes.Buffer, data []byte, chunkType ChunkType) error {
	// 设置header中的长度
	binary.LittleEndian.PutUint16(seg.header[4:6], uint16(len(data)))
	// 设置header中的类型
	seg.header[6] = chunkType

	// 对 len + type + data 求 checksum
	sum := crc32.ChecksumIEEE(seg.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	// 设置header中的校验和
	binary.LittleEndian.PutUint32(seg.header[:4], sum)
	//将一个完整的chunk写入buf中（header + payload 就是一个chunk）
	_, err := buf.Write(seg.header[:])
	if err != nil {
		return err
	}
	_, err = buf.Write(data)
	if err != nil {
		return err
	}
	return nil
}

// 将数据data写入到buf中
func (seg *segmentFile) writeBuffer(data []byte, buf *bytes.Buffer) (*ChunkPosition, error) {

	startBufferLen := buf.Len()
	if seg.closed {
		return nil, ErrClosed
	}

	padding := uint32(0)
	//1.判断block剩余空间过小（chunkheader都没法写入）
	if seg.lastBlockSize+chunkHeaderSize >= blockSize {
		// block的剩余空间过小，只写入【填充字节】
		size := blockSize - seg.lastBlockSize
		_, err := buf.Write(make([]byte, size))
		if err != nil {
			return nil, err
		}
		padding += size
		// 新block
		seg.lastBlockIndex++
		seg.lastBlockSize = 0
	}

	// 数据写入的起始位置
	pos := &ChunkPosition{
		SegmentFileId: seg.segmentFileId,
		BlockIndex:    seg.lastBlockIndex,
		ChunkOffset:   seg.lastBlockSize,
	}

	dataLen := uint32(len(data))
	//2.block剩余空间可以写入完整chunk数据
	if seg.lastBlockSize+chunkHeaderSize+dataLen <= blockSize {
		err := seg.appendChunk2Buffer(buf, data, ChunkTypeFull)
		if err != nil {
			return nil, err
		}
		pos.ChunkSize = dataLen + chunkHeaderSize
	} else { // 将data保存为多个chunk

		var (
			leftSize            = dataLen           // 剩余数据大小
			curBlockSize        = seg.lastBlockSize // block 占用的大小
			chunkNum     uint32 = 0                 // 记录分成了几块
		)

		for leftSize > 0 {
			chunkType := ChunkTypeMiddle // 默认都是中间的数据
			if leftSize == dataLen {     // 第一次
				chunkType = ChunkTypeStart
			}
			// block剩余可以保存的大小
			freeSize := blockSize - curBlockSize - chunkHeaderSize
			if freeSize >= leftSize { // 最后一次
				freeSize = leftSize // 剩下的数据可以全部保存
				chunkType = ChunkTypeEnd
			}

			// 截取data中的数据
			err := seg.appendChunk2Buffer(buf, data[dataLen-leftSize:dataLen-leftSize+freeSize], chunkType)
			if err != nil {
				return nil, err
			}
			chunkNum += 1
			// 剩余的数据更新
			leftSize -= freeSize
			// block 占用的大小更新
			curBlockSize = (curBlockSize + chunkHeaderSize + freeSize) % blockSize
		}
		// chunk 占用的大小
		pos.ChunkSize = chunkNum*chunkHeaderSize + dataLen
	}

	endBufferLen := buf.Len()
	// 校验用
	if pos.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		panic(fmt.Sprintf("wrong!!! the chunk size %d is not equal to the buffer len %d",
			pos.ChunkSize+padding, endBufferLen-startBufferLen))
	}

	// 更新seg中block相关的两个值
	seg.lastBlockSize += pos.ChunkSize // 追加的数据大小
	if seg.lastBlockSize >= blockSize {
		seg.lastBlockIndex += seg.lastBlockSize / blockSize
		seg.lastBlockSize = seg.lastBlockSize % blockSize
	}
	return pos, nil
}

// 将buf写入fd中
func (seg *segmentFile) writeBuffer2File(buf *bytes.Buffer) error {
	if seg.lastBlockSize > blockSize {
		panic("can not exceed the block size")
	}

	if _, err := seg.fd.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// 数据写入当前文件
func (seg *segmentFile) Write(data []byte) (*ChunkPosition, error) {
	if seg.closed {
		return nil, ErrClosed
	}
	lastBlockIndex := seg.lastBlockIndex
	lastBlockSize := seg.lastBlockSize

	var err error
	buf := DefaultBuffer.Get()
	defer func() {
		// 万一写入失败，恢复文件的游标值
		if err != nil {
			seg.lastBlockIndex = lastBlockIndex
			seg.lastBlockSize = lastBlockSize
		}
		DefaultBuffer.Put(buf)
	}()
	// 将数据临时写到buffer
	pos, err := seg.writeBuffer(data, buf)
	if err != nil {
		return nil, err
	}
	// 将buffer写入文件
	err = seg.writeBuffer2File(buf)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

// 从block中读取完整的数据
func (seg *segmentFile) Read(blockIndex uint32, chunkOffset uint32) ([]byte, error) {
	data, _, err := seg.readInternal(blockIndex, chunkOffset)
	return data, err
}

// 文件id | blockIndex
func (seg *segmentFile) getCacheKey(blockIndex uint32) uint64 {
	return uint64(seg.segmentFileId)<<32 | uint64(blockIndex)
}
func (seg *segmentFile) readInternal(blockIndex uint32, chunkOffset uint32) ([]byte, *ChunkPosition, error) {

	if seg.closed {
		return nil, nil, ErrClosed
	}

	var (
		result      []byte
		segmengSize = seg.Size()
		buf         = DefaultReadBuffer.Get()
		nextChunk   = &ChunkPosition{SegmentFileId: seg.segmentFileId}
	)

	defer func() {
		DefaultReadBuffer.Put(buf)
	}()

	for {
		// 当前block默认大小
		curBlockSize := int64(blockSize)
		// 当前block之前的大小
		beforeBlockSize := int64(blockIndex) * blockSize
		if beforeBlockSize+curBlockSize > segmengSize {
			// 当前block【实际大小】
			curBlockSize = segmengSize - beforeBlockSize
		}
		// chunk起始位置越界
		if int64(chunkOffset) >= curBlockSize { // curBlockSize 可能是负数，表示读取到文件尾部
			return nil, nil, io.EOF
		}

		var (
			ok          bool
			cachedBlock []byte
		)
		if seg.localCache != nil { // 判断缓存是否存在
			cachedBlock, ok = seg.localCache.Get(seg.getCacheKey(blockIndex))
		}

		if ok {
			copy(buf.block, cachedBlock)
		} else {
			// 读取一个block
			_, err := seg.fd.ReadAt(buf.block[0:curBlockSize], beforeBlockSize)
			if err != nil {
				return nil, nil, err
			}

			// 设置缓存
			if seg.localCache != nil && curBlockSize == blockSize {
				cache := make([]byte, blockSize)
				copy(cache, buf.block)
				seg.localCache.Add(seg.getCacheKey(blockIndex), cache)
			}
		}

		// 从block读chunk header
		copy(buf.header, buf.block[chunkOffset:chunkOffset+chunkHeaderSize])

		// payload length
		length := binary.LittleEndian.Uint16(buf.header[4:6])
		// 读取payload
		start := chunkOffset + chunkHeaderSize
		result = append(result, buf.block[start:start+uint32(length)]...)

		// 校验数据完整性
		oldSum := binary.LittleEndian.Uint32(buf.header[:4])
		end := chunkOffset + chunkHeaderSize + uint32(length)
		sum := crc32.ChecksumIEEE(buf.block[chunkOffset+4 : end])
		if sum != oldSum {
			return nil, nil, ErrInvalidCRC
		}

		chunkType := buf.header[6]

		if chunkType == ChunkTypeFull || chunkType == ChunkTypeEnd {
			// 下一个数据的起始位置
			nextChunk.BlockIndex = blockIndex
			nextChunk.ChunkOffset = end

			if end+chunkHeaderSize >= blockSize { // 说明有填充区
				nextChunk.BlockIndex++
				nextChunk.ChunkOffset = 0
			}
			break
		}

		blockIndex++
		chunkOffset = 0
	}

	return result, nextChunk, nil
}

func (seg *segmentFile) NewSegmentReader() *segmentReader {
	return &segmentReader{
		seg:         seg,
		blockidx:    0,
		chunkoffset: 0,
	}
}

// writeAll write batch data to the segment file.
func (seg *segmentFile) writeAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}

	// if any error occurs, restore the segment status
	originBlockNumber := seg.lastBlockIndex
	originBlockSize := seg.lastBlockSize

	// init chunk buffer
	buf := DefaultBuffer.Get()
	defer func() {
		if err != nil {
			seg.lastBlockIndex = originBlockNumber
			seg.lastBlockSize = originBlockSize
		}
		DefaultBuffer.Put(buf)
	}()

	// write all data to the chunk buffer
	var pos *ChunkPosition
	positions = make([]*ChunkPosition, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = seg.writeBuffer(data[i], buf)
		if err != nil {
			return
		}
		positions[i] = pos
	}
	// write the chunk buffer to the segment file
	if err = seg.writeBuffer2File(buf); err != nil {
		return
	}
	return
}

// *****************单个文件读取的进度******************
type segmentReader struct {
	seg         *segmentFile
	blockidx    uint32
	chunkoffset uint32
}

// 返回数据和当前数据的位置
func (segReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	if segReader.seg.closed {
		return nil, nil, ErrClosed
	}

	curChunk := &ChunkPosition{
		SegmentFileId: segReader.seg.segmentFileId,
		BlockIndex:    segReader.blockidx,
		ChunkOffset:   segReader.chunkoffset,
	}

	data, nextChunk, err := segReader.seg.readInternal(curChunk.BlockIndex, curChunk.ChunkOffset)
	if err != nil {
		return nil, nil, err
	}
	// 估值
	curChunk.ChunkSize = nextChunk.BlockIndex*blockSize + nextChunk.ChunkOffset -
		(curChunk.BlockIndex*blockSize + curChunk.ChunkOffset)

	// 下一个读取chunk起始位置（记录下来）
	segReader.blockidx = nextChunk.BlockIndex
	segReader.chunkoffset = nextChunk.ChunkOffset

	return data, curChunk, nil
}
