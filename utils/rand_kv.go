package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	//lock    = sync.Mutex{}
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("easydb-test-key-%09d", i))
}

func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("easydb-test-value-" + string(b))
}
