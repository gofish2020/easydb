package tinywal

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

// segment
const (
	//segment 第一个文件编号
	firstSegmentFileId = 1
	// segment file的权限
	segmentFileModePerm = 0644
)

// block
const (
	// 单个Block 32KB
	blockSize = 32 * KB
	//blockSize = 10 * B
)

// chunk
const (
	// 块头默认大小 4(checksum) + 2(数据大小) + 1(数据类型)
	chunkHeaderSize = 7
)

func BlockSize() int {
	return blockSize
}
