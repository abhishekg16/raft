package raft

import (
	"encoding/gob"
	"bytes"
	"log"
	"fmt"
	"os"
	db "github.com/abhishekg16/raft/dataBaseConnection"
)

// TODO : Increase the encoding facility 
type DBEncoder struct{
}

type DBInterface struct {
	 ecf *DBEncoder
}


func GetDBInterface() *DBInterface {
	var di DBInterface
	var e DBEncoder
	di.ecf = &e
	return & di 
}

func (e *DBEncoder)  EncodeKey(key *int64) ([]byte,error){
	buf_enc := new(bytes.Buffer)
	enc := gob.NewEncoder(buf_enc)
	enc.Encode(*key)
	tResult := buf_enc.Bytes()
	result := make([]byte, len(tResult))
	l := copy(result, tResult)
	var err error
	err = nil
	if l != len(tResult) {
		log.Println ("Encodoing Failed")
		err= fmt.Errorf("Encoding Failed")
		os.Exit(0)
	}
	return result, err
	 
}

func (e *DBEncoder)  DecodeKey (data []byte) (*int64,error){
	buf_dec := new(bytes.Buffer)
	dec := gob.NewDecoder(buf_dec)
	buf_dec.Write(data)
	var key int64
	err := dec.Decode(&key)
	if err != nil {
		log.Println(err)
	}
	return &key,nil
}

func (e *DBEncoder)  EncodeValue(lEntry *LogEntry) ([]byte,error){
	buf_enc := new(bytes.Buffer)
	enc := gob.NewEncoder(buf_enc)
	enc.Encode(*lEntry)
	tResult := buf_enc.Bytes()
	result := make([]byte, len(tResult))
	l := copy(result, tResult)
	var err error
	err = nil
	if l != len(tResult) {
		log.Println ("Encodoing Failed")
		err= fmt.Errorf("Encoding Failed")
		os.Exit(0)
	}
	return result, err
}

func (e *DBEncoder)  DecodeValue (data []byte) (*LogEntry,error){
	buf_dec := new(bytes.Buffer)
	dec := gob.NewDecoder(buf_dec)
	buf_dec.Write(data)
	var key LogEntry
	err := dec.Decode(&key)
	if err != nil {
		log.Println(err)
	}
	return &key,nil
}

func (di * DBInterface)OpenConnection(path string) (error) {
	return db.OpenConnection(path)
}

// return the LogEntry associated with a key
// if not key found then return nil
func (di *DBInterface )Get( key int64 ) (*LogEntry,error) {
	val, err := (di.ecf).EncodeKey(&key)
	if err != nil {
		log.Println("Raft DBInterface: enconding failed", err)
		return nil, err
	}
	lEntry , err := db.Get(val)
	if err != nil {
		log.Println("Raft DBInterface: Cound not fetch", err)
		return nil, err
	}
	if lEntry == nil {
		return nil, nil 
	}
	dCode, err  := (di.ecf).DecodeValue(lEntry) 
	if err != nil {
		log.Println("Raft DBInterface: Cound not Decode", err)
		return nil, err
	}
	return dCode , err
}

func (di * DBInterface)Put(key int64, value LogEntry) error {

	kVal, err := (di.ecf).EncodeKey(&key)
	if err != nil {
		log.Println("Raft DBInterface: enconding failed", err)
		return err
	}
	
	lVal, err := (di.ecf).EncodeValue(&value)
	if err != nil {
		log.Println("Raft DBInterface: enconding failed", err)
		return err
	}
	
	err = db.Put(kVal, lVal)
	if err != nil {
		log.Println("Raft DBInterface: Cound not Insert", err)
		return err
	}
	return nil
} 

func (di *DBInterface)GetTerm(key int64) (int64, error){

	lEntry, err := di.Get(key)
	if err != nil {

		return -1, err
	} 
	log.Println("Raft DBInterface: logEntry For Term ", lEntry )
	if lEntry != nil {
		return lEntry.Term, nil
	} 
	return -1, nil
}

func (di *DBInterface)GetLastLogIndex() int64 {
	val , ok := db.GetLastKey()
	if ok == false {
		return 0
	} 
	
	key , err :=(di.ecf).DecodeKey(val)
	if err != nil {
		log.Println("Raft DBInterface: Decoding failed", err)
		return -1
	}
	return *key
}

func (di *DBInterface) GetLastLogTerm() int64 {
	val, ok := db.GetLastValue()
	if ok == false {
		return 0
	}
	value , err := (di.ecf).DecodeValue(val)
	if err != nil {
		log.Println("Raft DBInterface: Count not Decode", err)
		return -1
	}
	return value.Term
}

// delete if key is present
func (di *DBInterface)Delete( key int64)error {
	kVal, err := (di.ecf).EncodeKey(&key)
	if err != nil {
		log.Println("Raft DBInterface: enconding failed", err)
		return err
	}
	err = db.DeleteEntry(kVal)
	return err 
}

func DestroyDatabase(path string) error {
	return db.DestroyDatabase(path)
}

func (di *DBInterface)CloseDB() {
	if LOG >= HIGH {
		log.Println("Closing DB")
	}
	db.Close()
}





