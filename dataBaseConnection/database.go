package database

import levigo "github.com/jmhodges/levigo"
import "log"

const (
	LOG_DB = true
)



var ro  *levigo.ReadOptions
var wo *levigo.WriteOptions 
var db *levigo.DB
var itr *levigo.Iterator
// if database does not exist create a new data base or opens up the existing one

func DestroyDatabase(path string) error {
	o := levigo.NewOptions()
	err := levigo.DestroyDatabase(path,o)
	o.Close()
	return err
}

func OpenConnection(path string) (error) {	
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3<<10))
	opts.SetCreateIfMissing(true)
	var err error
	db, err = levigo.Open(path,opts)
	ro = levigo.NewReadOptions()
	wo = levigo.NewWriteOptions()
	return err
}

func Get( key []byte ) ([]byte,error) {
	data , err := db.Get(ro,key)	
	return data, err
}

func Put(key []byte, value []byte) error {
	err := db.Put(wo,key,value)
	return err
}

// resturn the highest value of the key
// if not value present return false
func GetLastValue() ([]byte, bool) {
	itr = db.NewIterator(ro)
	itr.SeekToLast()
	if itr.Valid() == false{
		return nil, false
	}
	return itr.Value(), true
}

// return lastKey
func GetLastKey() ([]byte, bool) {
	itr = db.NewIterator(ro)
	itr.SeekToLast()
	if itr.Valid() == false{
		return nil, false
	}
	return itr.Key(), true
}

func DeleteEntry(key []byte) error {
	return db.Delete(wo,key)
}

func Close() {
	log.Println("Closing connection")
	ro.Close()
	wo.Close()
	db.Close()
	log.Println("Sucessfully Closed connection")
}


