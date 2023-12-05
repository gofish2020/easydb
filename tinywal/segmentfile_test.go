package tinywal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegmentFileName(t *testing.T) {
	t.Log(segmentFileName("/nash", ".log", 1))
}

func TestOpen(t *testing.T) {

	//t.Log(DefaultOption)
	_, err := Open(DefaultWalOption)
	assert.Nil(t, err)

	//t.Log(err)
}
