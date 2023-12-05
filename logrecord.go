package easydb

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal = iota
	LogRecordDeleted
	LogRecordBatchEnd
)

const maxLogRecordHeaderSize = 1 + binary.MaxVarintLen64 + binary.MaxVarintLen32 + binary.MaxVarintLen32

type LogRecord struct {
	Key     []byte
	Value   []byte
	Type    LogRecordType
	BatchId uint64
}

func NewLogRecord() *LogRecord {
	return &LogRecord{}
}

/*
序列化
type + batchid         +      keysize    +     valuesize    +      key   +    value
 1	   varint(max 10)         varint(max 5)    varint(max 5)
*/

func (l *LogRecord) Encode() []byte {
	header := make([]byte, maxLogRecordHeaderSize)
	header[0] = l.Type
	index := 1
	index += binary.PutUvarint(header[index:], l.BatchId)
	index += binary.PutVarint(header[index:], int64(len(l.Key)))
	index += binary.PutVarint(header[index:], int64(len(l.Value)))

	result := make([]byte, index+len(l.Key)+len(l.Value))

	//copy header
	copy(result, header[:index])
	// copy key
	copy(result[index:], l.Key)
	// copy value
	copy(result[index+len(l.Key):], l.Value)

	return result
}

func (l *LogRecord) Decode(data []byte) {

	l.Type = data[0] // 类型

	index := 1
	n := 0
	l.BatchId, n = binary.Uvarint(data[index:])
	index += n
	keyLen, n := binary.Varint(data[index:])
	index += n
	valueLen, n := binary.Varint(data[index:])
	index += n

	key := make([]byte, keyLen)
	copy(key, data[index:index+int(keyLen)])
	index += int(keyLen)

	value := make([]byte, valueLen)
	copy(value, data[index:index+int(valueLen)])

	l.Key = key
	l.Value = value
}
