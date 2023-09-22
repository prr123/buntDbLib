// test program for buntLib

package buntLib

import (
	"log"
//	"fmt"
	"testing"
	"os"
    "math/rand"
    "time"
)

func TestAddEntry(t *testing.T) {

	_, err := os.Stat("testDb")
	if err == nil {
		err1 := os.RemoveAll("testDb")
		if err1 != nil {t.Errorf("error -- could not remove files: %v", err1)}
	}

	bdb, err := InitDb("testDb", false)
	if err != nil {t.Errorf("error -- InitDb: %v", err)}

	err = bdb.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	valstr, err := bdb.GetValue("key1")
	if err != nil {t.Errorf("error -- GetVal: %v", err)}
	if valstr != "val1" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1")}

}


func TestDelEntry(t *testing.T) {

	kv, err := InitDb("testDb", false)
	if err != nil {t.Errorf("error -- InitDb: %v", err)}

	err = kv.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	valstr, err := kv.GetValue("key1")
	if err != nil {t.Errorf("error -- GetVal: %v", err)}
	if valstr != "val1" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1")}

	err = kv.DelEntry("key1")
	if err != nil {t.Errorf("error -- DelEntry: %v", err)}

	valstr, err = kv.GetValue("key1")
	if err != nil {t.Errorf("error -- GetValue found value %s for deleted key key1", valstr)}
	if len(valstr) > 0 {t.Errorf("valstr found for key1:[%d] %s", len(valstr), valstr)}
//	log.Printf("valstr key1: %s\n", valstr)

	valstr, err = kv.GetValue("key2")
	if err != nil {t.Errorf("error -- GetValue found value %s for key key2", valstr)}
	if len(valstr) > 0 {t.Errorf("valstr found for key2:[%d] %s", len(valstr), valstr)}
//	log.Printf("valstr key2: %s\n", valstr)
}

func TestNumEntries(t *testing.T) {

//	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
    os.RemoveAll("testDbNew")

	numEntries := 100
	kv, err := InitDb("testDbNew", false)
    if err != nil {t.Errorf("error -- InitDb: %v", err)}

    err = kv.FillRan(numEntries)
    if err != nil {t.Errorf("error -- FillRan: %v", err)}

	num, err := kv.GetNumEntries()
	if err != nil {t.Errorf("error -- GetNumEntries: %v", err)}
	if num != numEntries {t.Errorf("error -- entry %d != %d", numEntries, num)}

//    keys, err := kv.ListKeys()
//	if len(keys) != num {t.Errorf("error ListKeys: %d", len(keys))}
}


func TestRanEntries(t *testing.T) {

    var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

    os.RemoveAll("testDbNew")

    numEntries := 10
    kv, err := InitDb("testDbNew", false)
    if err != nil {t.Errorf("error -- InitDb: %v", err)}

    err = kv.FillRan(numEntries)
    if err != nil {t.Errorf("error -- FillRan: %v", err)}

    keys, err := kv.ListKeys()
    if err != nil {t.Errorf("error -- ListKeys: %v", err)}
//	log.Printf("keys %d\n", len(keys))


	if len(keys) != numEntries {t.Errorf("error number of keys %d do not match!", len(keys))}

    for n := 0; n < 10; n++ {
        kidx := seededRand.Intn(numEntries)
        keyStr := keys[kidx]
        valstr, err := kv.GetValue(keyStr)
		if err != nil {t.Errorf("error iter[%d] GetValue: %v", n, err)}
        if len(valstr) < 1 {t.Errorf("key[%d] %s: invalid valstr!", kidx, keyStr)}
    }

}

/*
func TestBckupAndLoad(t *testing.T) {

	kv, err := InitDb("testDb", false)
	if err != nil {t.Errorf("error -- InitDb: %v", err)}

	err = kv.FillRan(5)
	if err != nil {t.Errorf("error -- FillRan: %v", err)}

	err = kv.Backup("testBackup.dat")
	if err != nil {t.Errorf("error -- Backup: %v", err)}

	kvnew, err := InitDb("testDb", false)
	if err != nil {t.Errorf("error -- Load: %v", err)}

	err = kvnew.Load("azulkvBase.dat")
	if err != nil {t.Errorf("error -- Load: %v", err)}

	if (*kv.Entries) != (*kvnew.Entries) {t.Errorf("error entries do not match kv: %d kvnew: %d", (*kv.Entries), (*kvnew.Entries))}
	for i:=0; i< (*kv.Entries); i++ {
		if (*kv.Keys)[i] != (*kvnew.Keys)[i] {
			t.Errorf("error -- no key match at idx[%d] key: %s keynew: %s",i, (*kv.Keys)[i], (*kvnew.Keys)[i])
		}
	}
//	err = os.Remove("testDb/testBackup.dat")
//	if err != nil {t.Errorf("error -- Remove: %v", err)}

}
*/


func BenchmarkGet(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	os.RemoveAll("testDbNew")

	numEntries := 100
	kv, err := InitDb("testDbNew", false)
    if err != nil {log.Fatalf("error -- InitDb: %v", err)}

    err = kv.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

	keys, err := kv.ListKeys()
    if err != nil {log.Fatalf("error -- ListKeys: %v", err)}


	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := keys[kidx]
		valstr, err := kv.GetValue(keyStr)
		if err != nil {log.Fatalf("error iter[%d] GetValue: %v", n, err)}
		if len(valstr) < 1 {log.Fatalf("key[%d] %s: invalid valstr!", kidx, keyStr)}
	}
}

