// buntTest
//

package main

import (
	"os"
	"fmt"
	bunt "github.com/tidwall/buntdb"
)

func main () {

	path := "db/bunt.db"
	db, err := bunt.Open(path)
	if err != nil {
  		fmt.Printf("error opening bunt: %v\n", err)
		os.Exit(-1)
	}
	defer db.Close()
	fmt.Println("success opening buntdb!")
}
