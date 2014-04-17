package kvstore

/** This is the interfece for the KV storage. The raft layer does not know any 
 thing about the command. This layers take a command and execute that on the underlaying 
 storage and then return back the result.
 
 The layer supports following commands
 1. Get (key) -> value , bool
 2. Put (key , value) -> bool
 3. Delete (key) -> bool 
 
 
 The Command would be contained in following struct 
 type Command struct {
 	Cmd string
 	Key []byte
 	value []byte
 }
 
 The present KVstore only assume that the key values are uninterprested byte array.
 The user can easily modify this interface inorder to attach a different kind of KVstore 
 **/



import (
	db "github.com/abhishekg16/raft/dataBaseConnection"
	"fmt"
)

// Type of the command supported by the Key-Value storage
const (
	Get = iota
	Put = iota
	Del = iota
)

// KVInterface bound the connection object with Key-Value storage
type KVInterface struct{
	conn *db.DbConnection 
}

// Command struct used to passe the command to this layer
type Command struct {
 	Cmd int
 	Key []byte
 	Value []byte
}

// GetKVInterface method give an instance by using which raft layer can communicate with the
// key value storage 
func GetKVInterface(path string) (*KVInterface , error){
	 var kvi KVInterface
	 kvi.conn = db.InitializeNewConnection()
	 err := kvi.conn.OpenOldConnection(path)
	 if (err != nil) {
	 	return nil, err
	 }  
	 return  &kvi, nil
}

// ExecuteCommand method execute the command on the KV store and return
// error and result in the []byte array
func (kvi *KVInterface)ExecuteCommand(cmd * Command) ([]byte, error){

	if cmd.Cmd == Get {
	   value, err :=	kvi.conn.Get(cmd.Key)
	   return value, err
	
	} else if cmd.Cmd == Put {
		err := kvi.conn.Put(cmd.Key,cmd.Value)
		return nil , err
	} else if cmd.Cmd == Del {
		err := kvi.conn.DeleteEntry(cmd.Key)
		return nil,err
	} else {
		err := fmt.Errorf("KvstoreInterface: Command Does not supported")
		return  nil, err
	} 
	return nil,nil
}

// CloseKV free all allocated objects and close the connection with the underlying KeyValue 
// layer
func (kvi *KVInterface) CloseKV() {
	kvi.conn.Close()
}

