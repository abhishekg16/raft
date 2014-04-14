package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	db "github.com/abhishekg16/raft/dataBaseConnection"
	"log"
	"os"
)

// TODO : Increase the encoding facility
type DBEncoder struct {
}

type DBInterface struct {
	ecf    *DBEncoder
	conn   *db.DbConnection
	id     int
	logger *log.Logger
}

func GetDBInterface(id int, logger *log.Logger) *DBInterface {
	t_conn := db.InitializeNewConnection()
	var di DBInterface
	var e DBEncoder
	di.ecf = &e
	di.conn = t_conn
	di.id = id
	di.logger = logger
	return &di
}

func (e *DBEncoder) EncodeKey(key *int64) ([]byte, error) {
	buf_enc := new(bytes.Buffer)
	enc := gob.NewEncoder(buf_enc)
	enc.Encode(*key)
	tResult := buf_enc.Bytes()
	result := make([]byte, len(tResult))
	l := copy(result, tResult)
	var err error
	err = nil
	if l != len(tResult) {
		log.Println("Encodoing Failed")
		err = fmt.Errorf("Encoding Failed")
		os.Exit(0)
	}
	return result, err

}

func (e *DBEncoder) DecodeKey(data []byte) (*int64, error) {
	buf_dec := new(bytes.Buffer)
	dec := gob.NewDecoder(buf_dec)
	buf_dec.Write(data)
	var key int64
	err := dec.Decode(&key)
	if err != nil {
		log.Println(err)
	}
	return &key, nil
}

func (e *DBEncoder) EncodeValue(lEntry *LogEntry) ([]byte, error) {
	buf_enc := new(bytes.Buffer)
	enc := gob.NewEncoder(buf_enc)
	enc.Encode(*lEntry)
	tResult := buf_enc.Bytes()
	result := make([]byte, len(tResult))
	l := copy(result, tResult)
	var err error
	err = nil
	if l != len(tResult) {
		log.Println("Encodoing Failed")
		err = fmt.Errorf("Encoding Failed")
		os.Exit(0)
	}
	return result, err
}

func (e *DBEncoder) DecodeValue(data []byte) (*LogEntry, error) {
	buf_dec := new(bytes.Buffer)
	dec := gob.NewDecoder(buf_dec)
	buf_dec.Write(data)
	var key LogEntry
	err := dec.Decode(&key)
	if err != nil {
		log.Printf("Error :%v\n",err)
	}
	return &key, nil
}

func (di *DBInterface) OpenConnection(path string) error {
	return di.conn.OpenConnection(path)
}

// return the LogEntry associated with a key
// if not key found then return nil
func (di *DBInterface) Get(key int64) (*LogEntry, error) {
	val, err := (di.ecf).EncodeKey(&key)
	if err != nil {
		di.logger.Println("Raft DBInterface: enconding failed", err)
		return nil, err
	}
	lEntry, err := di.conn.Get(val)
	if err != nil {
		di.logger.Printf("Raft DBInterface %v: Cound not fetch Error : %v\n", di.id, err)
		return nil, err
	}
	if lEntry == nil {
		return nil, nil
	}
	dCode, err := (di.ecf).DecodeValue(lEntry)
	if err != nil {
		di.logger.Printf("Raft DBInterface %v: Cound not Decode Error : %v\n", di.id, err)
		return nil, err
	}
	return dCode, err
}

func (di *DBInterface) Put(key int64, value LogEntry) error {

	kVal, err := (di.ecf).EncodeKey(&key)
	if err != nil {
		di.logger.Println("Raft DBInterface %v: enconding failed Error: %v", di.id, err)
		return err
	}

	lVal, err := (di.ecf).EncodeValue(&value)
	if err != nil {
		di.logger.Println("Raft DBInterface %v: enconding failed, Error: %v", di.id, err)
		return err
	}

	err = di.conn.Put(kVal, lVal)
	if err != nil {
		di.logger.Println("Raft DBInterface %v: Cound not Insert, Error: %v", di.id, err)
		return err
	}
	if LOG >= FINE {
		di.logger.Printf("Raft DBInterface %v: Put : Index %v, Entry %v", di.id, key, value)
	}
	return nil
}

func (di *DBInterface) GetTerm(key int64) (int64, error) {
	lEntry, err := di.Get(key)
	if err != nil {
		return -1, err
	}
	if LOG >= FINE {
		di.logger.Printf("Raft DBInterface %v: GET: logEntry For Index %v : %+v \n", di.id, key, lEntry)
	}
	if lEntry != nil {
		return lEntry.Term, nil
	}
	return -1, nil
}

func (di *DBInterface) GetLastLogIndex() int64 {
	val, ok := di.conn.GetLastKey()
	if ok == false {
		return 0
	}

	key, err := (di.ecf).DecodeKey(val)
	if err != nil {
		di.logger.Println("Raft DBInterface %v: Decoding failed %v \n", di.id, err)
		return -1
	}
	return *key
}

func (di *DBInterface) GetLastLogTerm() int64 {
	val, ok := di.conn.GetLastValue()
	if ok == false {
		return 0
	}
	value, err := (di.ecf).DecodeValue(val)
	if err != nil {
		di.logger.Println("Raft DBInterface: Count not Decode", err)
		return -1
	}
	return value.Term
}

// delete if key is present
func (di *DBInterface) Delete(key int64) error {
	kVal, err := (di.ecf).EncodeKey(&key)
	if err != nil {
		di.logger.Println("Raft DBInterface: enconding failed", err)
		return err
	}
	err = di.conn.DeleteEntry(kVal)
	return err
}

func DestroyDatabase(path string) error {
	return db.DestroyDatabase(path)
}

func (di *DBInterface) CloseDB() {
	if LOG >= HIGH {
		di.logger.Println("Closing DB")
	}
	di.conn.Close()
}

func (di *DBInterface) PrintLog() {
	di.conn.PrintLogInit()
	for {
		b_key, b_value, ok := di.conn.Print()
		if ok == false {
			di.conn.PrintLogClose()
			return
		}
		key, err := (di.ecf).DecodeKey(b_key)
		if err != nil {
			di.logger.Println("Raft DBInterface: decoding failed", err)
			continue
		}
		value, err := (di.ecf).DecodeValue(b_value)
		if err != nil {
			di.logger.Println("Raft DBInterface: decoding failed", err)
			continue
		}
		di.logger.Printf("{ %v -> %v }", *key, value)
	}
}
