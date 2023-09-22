// bunttLib
// a library that facilitates using buntdb
//
// Author: prr, azulsoftware
// Date: 4. September 2023
// copyright 2023 prr, azul software

package buntLib

import (
    "fmt"
//    "log"
    "os"
	"math/rand"
	"time"

//	yaml "github.com/goccy/go-yaml"
    buntdb "github.com/tidwall/buntdb"
)

type DBObj struct {
     Db *buntdb.DB
	 Dbg bool
    }


func InitDb(dbPath string, dbg bool)(db *DBObj, err error) {

//    path := "boltTest.db"
    dbobj, err := buntdb.Open(dbPath)
    if err != nil {
        return nil, fmt.Errorf("bunt.Open: %v\n", err)
    }
    dbObj := DBObj {
        Db: dbobj,
		Dbg: dbg,
    }

    return &dbObj, nil
}

func (dbobj *DBObj) DbClose() (err error){

    db := dbobj.Db
	err = db.Close()
	return err
}


func (dbObj *DBObj) DbConfig(filnam string) (err error) {

	if len(filnam) == 0 {filnam = "$gosrc/db/bunt/buntConfig/config.yaml"}

	cdat, err := os.ReadFile(filnam)
	if err != nil {return fmt.Errorf("ReadFile: %v", err)}

	fmt.Printf("cdat: %s\n", string(cdat))
	return nil
}


func (dbobj *DBObj) AddEntry(key, val string) (err error) {

    db := dbobj.Db
    err = db.Update(func(tx *buntdb.Tx) error {
        _,_, err := tx.Set(key, val, nil)
        if err != nil {
            return fmt.Errorf("could not insert entry: %v", err)
        }
        return nil
    })
    return err
}

func (dbobj *DBObj) GetValue(key string) (val string, err error) {

    db := dbobj.Db
	valStr := ""
    err = db.View(func(tx *buntdb.Tx) error {
		// the second parameter indicates whether to igmore the time stamp
		valStr, err = tx.Get(key, true)
        if err != nil  {
            return fmt.Errorf("error get:%v!", err)
        }
        return nil
    })
    return valStr, nil
}

func (dbobj *DBObj) DelEntry(key string) (err error) {

    db := dbobj.Db

	err = db.Update(func(tx *buntdb.Tx) error {
		// need to test
		//dvalStr, err := tx.Delete(key)
		_, err := tx.Delete(key)
		if err != nil {return fmt.Errorf("could not delete key: %v", err)}
		//fmt.Printf("dvalStr: %s\n", dvalStr)
    	return nil
	})
    return err
}

func (dbobj *DBObj) UpdEntry(key, val string) (err error) {

    db := dbobj.Db

	err = db.Update(func(tx *buntdb.Tx) error {
		valStr, err := tx.Get(key)
        if err != nil  {
            return fmt.Errorf("tx.get: %v!", err)
        }
		if len(valStr) == 0 { return fmt.Errorf("key does not exist!")}
        _,_, err = tx.Set(key, val, nil)
        if err != nil {
            return fmt.Errorf("tx.Set: %v", err)
        }
    	return nil
	})
    return err
}



func (dbobj *DBObj) ListKeys() (keyList []string, err error) {

    db := dbobj.Db

	err = db.View(func(tx *buntdb.Tx) error {
    	err = tx.Ascend("", func(key, value string) bool {
//        	fmt.Printf("key: %s, value: %s\n", key, value)
			keyList = append(keyList,key)
       	 return true // continue iteration
    	})

   	return err
    })

	return keyList, err
//    return keyList, err
}


func (dbobj *DBObj) FillRan (level int) (err error){

    db := dbobj.Db
	// need to update with bulk insertion
	err = db.Update(func(tx *buntdb.Tx) error {
    	for i:=0; i<level; i++ {
        	keydat := GenRanData(5, 25)
        	valdat := GenRanData(5, 40)
        	valstr := fmt.Sprintf("val-%d_%s",i,string(valdat))
        	_,_, err := tx.Set(string(keydat), valstr, nil)
        	if err != nil {
            	return fmt.Errorf("could not insert entry[%d]: %v", i, err)
        	}
    	}
        return nil
	})

    return err
}


func (dbobj *DBObj) GetNumEntries () (num int, err error){

    db := dbobj.Db
    err = db.View(func(tx *buntdb.Tx) error {
		num, err =tx.Len()
		if err != nil {return fmt.Errorf("Len: %v", err)}
        return nil
	})
	return num, nil
}

func (dbobj *DBObj) Backup (tabNam string) (err error){
//    db := dbobj.Db

	return nil
}

func (dbobj *DBObj) Load(tabNam string) (err error){
//    db := dbobj.Db

	return nil
}


func GenRanData (rangeStart, rangeEnd int) (bdat []byte) {

    var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//    rangeStart := 5
//    rangeEnd := 25
    offset := rangeEnd - rangeStart

    randLength := seededRand.Intn(offset) + rangeStart
    bdat = make([]byte, randLength)

    charset := "abcdefghijklmnopqrstuvw0123456789"
    for i := range bdat {
        bdat[i] = charset[seededRand.Intn(len(charset)-1)]
    }
    return bdat
}


func PrintList(Title string, namList []string) {

    fmt.Printf("********* %s ********\n", Title)
    for i:=0; i< len(namList); i++ {
        fmt.Printf("  %d: %s\n", i+1, namList[i])
    }
    fmt.Printf("******* end %s ******\n", Title)
}
