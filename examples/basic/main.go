package main

import (
	"fmt"

	"github.com/gofish2020/easydb"
	"github.com/gofish2020/easydb/utils"
)

// this file shows how to use the basic operations of EasyDB
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

	// put a key
	err = db.Put([]byte("name"), []byte("easydb"), nil)
	if err != nil {
		panic(err)
	}

	// get a key
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	// delete a key
	err = db.Delete([]byte("name"), nil)
	if err != nil {
		panic(err)
	}

	// get a key
	val, err = db.Get([]byte("name"))
	if err != nil {
		if err == easydb.ErrKeyNotFound {
			fmt.Println("key not exist")
			return
		}
		panic(err)
	}
	println(string(val))
}
