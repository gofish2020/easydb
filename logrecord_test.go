package easydb

import (
	"testing"

	"github.com/gofish2020/easydb/utils"
	"github.com/stretchr/testify/assert"
)

func TestLogRecord(t *testing.T) {

	logRecord := &LogRecord{
		Key:     utils.GetTestKey(10),
		Value:   utils.RandomValue(10),
		Type:    LogRecordNormal,
		BatchId: 1911888181881,
	}

	data := logRecord.Encode()

	newRecord := &LogRecord{}

	newRecord.Decode(data)

	assert.Equal(t, logRecord.Key, newRecord.Key)
	assert.Equal(t, logRecord.Value, newRecord.Value)
	assert.Equal(t, logRecord.Type, newRecord.Type)
	assert.Equal(t, logRecord.BatchId, newRecord.BatchId)

	t.Log(string(newRecord.Key))
	t.Log(string(newRecord.Value))
	t.Log(newRecord.Type)
	t.Log(newRecord.BatchId)
}
