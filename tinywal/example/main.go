package main

import (
	"fmt"
	"io"

	"github.com/gofish2020/easydb/tinywal"
	"github.com/gofish2020/easydb/utils"
)

func main() {

	option := tinywal.DefaultWalOption
	option.Dir = utils.ExecDir() + "/data"
	option.SegmentSize = 20 * tinywal.B // 设置文件大小20B
	option.LocalCacheSize = uint64(2 * tinywal.BlockSize())
	option.IsSync = false
	option.BytesPerSync = 0

	wal, err := tinywal.Open(option)
	if err != nil {
		return
	}

	pos, err := wal.Write([]byte("333444")) // 写入 13 字节
	if err != nil {
		return
	}

	result, err := wal.Read(pos)
	if err != nil && err != io.EOF {
		return
	}
	fmt.Println(result)

	result1, err := wal.Read(pos)
	if err != nil && err != io.EOF {
		return
	}
	fmt.Println(result1)

}
