package tinywal

import "io"

type Reader struct {
	allSegmentReader []*segmentReader // 所有文件
	progress         int              // 文件进度
}

func (r *Reader) Next() ([]byte, *ChunkPosition, error) {

	if r.progress >= len(r.allSegmentReader) { // 所有文件都完成读
		return nil, nil, io.EOF
	}
	
	data, chunkPos, err := r.allSegmentReader[r.progress].Next()
	if err == io.EOF { // 说明当前文件读取到尾部
		r.progress++    // 继续下一个文件
		return r.Next() // 从中读取数据
	}

	return data, chunkPos, nil
}
