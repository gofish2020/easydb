package tinywal

import "errors"

var ErrDataTooLarge = errors.New("数据太大")

var ErrClosed = errors.New("文件已关闭")

var ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")

var ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
