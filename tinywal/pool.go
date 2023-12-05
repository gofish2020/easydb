package tinywal

import (
	"bytes"
	"sync"
)

// 写 缓存
type bufferPool struct {
	buffer sync.Pool
}

var DefaultBuffer = newBufferPool()

func newBufferPool() *bufferPool {
	return &bufferPool{
		buffer: sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}
}

func (b *bufferPool) Get() *bytes.Buffer {
	return b.buffer.Get().(*bytes.Buffer)
}

func (b *bufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	b.buffer.Put(buf)
}

// 读 缓存

var DefaultReadBuffer = newReadbufferPool()

func newReadbufferPool() *readBufferPool {
	return &readBufferPool{
		buffer: sync.Pool{
			New: newBlockAndHeader,
		},
	}
}

type readBufferPool struct {
	buffer sync.Pool
}

func (b *readBufferPool) Get() *blockAndHeader {
	return b.buffer.Get().(*blockAndHeader)
}

func (b *readBufferPool) Put(buf *blockAndHeader) {
	b.buffer.Put(buf)
}

type blockAndHeader struct {
	block  []byte
	header []byte
}

func newBlockAndHeader() interface{} {
	return &blockAndHeader{
		block:  make([]byte, blockSize),       // block 32KB
		header: make([]byte, chunkHeaderSize), // chunk 头 7字节
	}
}
