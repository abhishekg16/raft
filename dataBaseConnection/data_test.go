package database

import "testing" 
import "log"

func Test_DB(t *testing.T) {
	err := DestroyDatabase("../data/db")
	if err != nil {
		t.Errorf("Could not destroy previous databese %v\n", err)
	}
	err = OpenConnection("../data/db")
	if err != nil {
		t.Errorf("Could not open database %v\n", err)
		
	}
	defer Close()
	err = Put([]byte("hi"),[]byte("bye"))
	if err != nil {
		t.Errorf("Could not put the value %v \n", err)
	}
	
	val , err := Get([]byte("hi"))
	if err != nil {
		t.Errorf("Cound not Get the value %v \n", err)
	}
	value := string(val)
	if value != "bye" {
		t.Errorf("Cound not fetch same value")
	}
	
	err = Put([]byte("hi1"),[]byte("bye1"))
	err = Put([]byte("hi2"),[]byte("bye2"))
	lastKey , _ :=	GetLastKey()
	lastVal , _ := GetLastValue()
	log.Println(string(lastKey))
	log.Println(string(lastVal))	
	
	if string(lastKey) != "hi2" {
		t.Errorf("Cound match last key")
	}
	if string(lastVal) != "bye2" {
		t.Errorf("Cound match last val")
	}  
} 
