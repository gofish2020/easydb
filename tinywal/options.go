package tinywal

type Options struct {
	// 存放segment文件目录
	Dir string
	// Segment 文件后缀名，例如：.log / .seg
	SegmentFileExt string
	// 单个Segment文件最大大小
	SegmentSize int64
	// 缓存大小(unit : Byte)，用来缓存最近读取过的Block（所有文件共用的缓存，不是每个文件一个缓存）
	LocalCacheSize uint64
	// 是否将os pagecache中的数据，刷到磁盘中
	IsSync bool
	//每写入BytesPerSync字节，主动将os pagecache刷入磁盘中
	BytesPerSync uint64
}

var DefaultWalOption = Options{
	Dir:            "/Users/mac/easydb/data",
	SegmentFileExt: ".log",
	SegmentSize:    1 * GB,
	LocalCacheSize: 500 * MB,
	IsSync:         true,
	BytesPerSync:   0,
}
