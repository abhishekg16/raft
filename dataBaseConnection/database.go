package database

import levigo "github.com/jmhodges/levigo"

//import "log"

const (
	LOG_DB = true
)

type DbConnection struct {
	ro  *levigo.ReadOptions
	wo  *levigo.WriteOptions
	db  *levigo.DB
	itr *levigo.Iterator
}

// if database does not exist create a new data base or opens up the existing one

func InitializeNewConnection() *DbConnection {
	var conn DbConnection
	return &conn
}

func DestroyDatabase(path string) error {
	o := levigo.NewOptions()
	err := levigo.DestroyDatabase(path, o)
	o.Close()
	return err
}


func (conn *DbConnection) OpenOldConnection(path string) error {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 10))
	opts.SetCreateIfMissing(false)
	var err error
	conn.db, err = levigo.Open(path, opts)
	conn.ro = levigo.NewReadOptions()
	conn.wo = levigo.NewWriteOptions()
	return err
}

func (conn *DbConnection) OpenConnection(path string) error {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 10))
	opts.SetCreateIfMissing(true)
	var err error
	conn.db, err = levigo.Open(path, opts)
	conn.ro = levigo.NewReadOptions()
	conn.wo = levigo.NewWriteOptions()
	return err
}

func (conn *DbConnection) Get(key []byte) ([]byte, error) {
	data, err := (conn.db).Get(conn.ro, key)
	return data, err
}

func (conn *DbConnection) Put(key []byte, value []byte) error {
	err := conn.db.Put(conn.wo, key, value)
	return err
}

// resturn the highest value of the key
// if not value present return false
func (conn *DbConnection) GetLastValue() ([]byte, bool) {
	conn.itr = conn.db.NewIterator(conn.ro)
	conn.itr.SeekToLast()
	if conn.itr.Valid() == false {
		return nil, false
	}
	return conn.itr.Value(), true
}

// return lastKey
func (conn *DbConnection) GetLastKey() ([]byte, bool) {
	conn.itr = conn.db.NewIterator(conn.ro)
	defer conn.itr.Close()
	conn.itr.SeekToLast()
	if conn.itr.Valid() == false {
		return nil, false
	}
	return conn.itr.Key(), true
}

func (conn *DbConnection) DeleteEntry(key []byte) error {
	return conn.db.Delete(conn.wo, key)
}

func (conn *DbConnection) Close() {
	//log.Println("Closing connection")
	conn.ro.Close()
	conn.wo.Close()
	conn.db.Close()
	//log.Println("Sucessfully Closed connection")
}

func (conn *DbConnection) PrintLogInit() {
	conn.itr = conn.db.NewIterator(conn.ro)
	conn.itr.SeekToFirst()
}

func (conn *DbConnection) Print() ([]byte, []byte, bool) {
	if conn.itr.Valid() == false {
		return nil, nil, false
	}
	key := conn.itr.Key()
	value := conn.itr.Value()
	conn.itr.Next()
	return key, value, true
}

func (conn *DbConnection) PrintLogClose() {
	conn.itr.Close()
}
