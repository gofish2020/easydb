package tinywal

type ChunkType = byte

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeStart
	ChunkTypeMiddle
	ChunkTypeEnd
)

type ChunkPosition struct {
	// 文件id
	SegmentFileId SegmentFileId
	// block索引
	BlockIndex uint32
	// 在block内到偏移位置
	ChunkOffset uint32
	// chunk写入，实际占用的总大小
	ChunkSize uint32
}
