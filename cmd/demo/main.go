package main

import (
	"fmt"
	"os"

	"github.com/apteva/aptevadb"
)

func main() {
	dir := os.Args[1]
	os.RemoveAll(dir)
	db, err := aptevadb.Open(dir, aptevadb.Options{})
	if err != nil {
		panic(err)
	}
	db.InsertJSON([]byte("order:1"), []byte(`{"region":"north","amount":12.5}`))
	db.InsertJSON([]byte("order:2"), []byte(`{"region":"east","amount":99}`))
	// leave one record only in the log: insert after the checkpoint, flush, no Close
	db.Checkpoint()
	db.InsertJSON([]byte("order:3"), []byte(`{"region":"west","amount":7}`))
	db.FlushLog()
	fmt.Println(db.Stats())
}
