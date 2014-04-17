package database
/*
database package communicate with KeyValue store used for logging. In this implementation
we are using the levelDB. 
This package provides provide easy interface to allow the establish connection , create new log
database and destroy log data base etc.  
*/

import levigo "github.com/jmhodges/levigo"

//import "log"

const (
	LOG_DB = true
)
// DbConntion object encapsulate all the facilities. provied by this package
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

// This method is used to destroy the prevoius existing database. This method completely
// removes the previous instance
func DestroyDatabase(path string) error {
	o := levigo.NewOptions()
	err := levigo.DestroyDatabase(path, o)
	o.Close()
	return err
}

// OpenOldConnection method open connection to the database whose path is passed 
// as parameters. In case previous instance does not exist it returns the error  
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

// OpenConnection open the connection to the database instance. In case database instance 
// does not exist in that case it simply create the a new database instance
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

// Get calls the Get method of underlylying keycalue store
func (conn *DbConnection) Get(key []byte) ([]byte, error) {
	data, err := (conn.db).Get(conn.ro, key)
	return data, err
}

// Put call the put method of the underlying key calue store
func (conn *DbConnection) Put(key []byte, value []byte) error {
	err := conn.db.Put(conn.wo, key, value)
	return err
}

// GetLastValue resturn the highest value of the key
// if not value present return false
func (conn *DbConnection) GetLastValue() ([]byte, bool) {
	conn.itr = conn.db.NewIterator(conn.ro)
	conn.itr.SeekToLast()
	if conn.itr.Valid() == false {
		return nil, false
	}
	return conn.itr.Value(), true
}

// GetLastKey return lastKey
func (conn *DbConnection) GetLastKey() ([]byte, bool) {
	conn.itr = conn.db.NewIterator(conn.ro)
	defer conn.itr.Close()
	conn.itr.SeekToLast()
	if conn.itr.Valid() == false {
		return nil, false
	}
	return conn.itr.Key(), true
}

// DeleteEntry calls the delete method of the underlying kayValue storage 
func (conn *DbConnection) DeleteEntry(key []byte) error {
	return conn.db.Delete(conn.wo, key)
}


// Close method free all allocated object and close the connection to the database
func (conn *DbConnection) Close() {
	//log.Println("Closing connection")
	conn.ro.Close()
	conn.wo.Close()
	conn.db.Close()
	//log.Println("Sucessfully Closed connection")
}

// Following method traverse all the Key-Value pair present in the underlying
// Key-Value storage. This mehtod should be used with case. This method should 
// called only when the log size is very small 

// PrintLogInit seek the pointer to starting position
func (conn *DbConnection) PrintLogInit() {
	conn.itr = conn.db.NewIterator(conn.ro)
	conn.itr.SeekToFirst()
}

// Print returns the key-Value pointed by the current key_value pointed by the conn.itr
func (conn *DbConnection) Print() ([]byte, []byte, bool) {
	if conn.itr.Valid() == false {
		return nil, nil, false
	}
	key := conn.itr.Key()
	value := conn.itr.Value()
	conn.itr.Next()
	return key, value, true
}

// PrintLogClose close the iterator.
func (conn *DbConnection) PrintLogClose() {
	conn.itr.Close()
}
