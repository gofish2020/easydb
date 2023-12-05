package main

import (
	"github.com/gofish2020/easydb"
	"github.com/gofish2020/easydb/utils"
)

// this file shows how to use the batch operations of EasyDB
func main() {
	// specify the options
	options := easydb.DefaultOptions
	options.DirPath = utils.ExecDir() + "/data"

	// open a database
	db, err := easydb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// create a batch
	batch := db.NewBatch(easydb.DefaultBatchOptions)

	// set a key
	_ = batch.Put([]byte("name"), []byte("EasyDB"))

	// get a key
	val, _ := batch.Get([]byte("name"))
	println(string(val))

	// delete a key
	_ = batch.Delete([]byte("name"))

	// commit the batch
	_ = batch.Commit(nil)

	// _ = batch.Put([]byte("name1"), []byte("EasyDB1")) // don't do this!!!
}
