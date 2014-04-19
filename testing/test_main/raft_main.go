package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	cluster "github.com/abhishekg16/cluster"
	raft "github.com/abhishekg16/raft"
	"log"
	"os"
	"time"
	"strconv"
	db "github.com/abhishekg16/raft/dataBaseConnection"
)

const (
	kvDir = "/kvstore"
	kvName = "kv"	
	LOG_LEVEL = 0
)


func init() {
	gob.Register(raft.Command{})
	gob.Register(raft.LogItem{})
}

// This main funtion starts the raft servers
func main() {
	var pid int
	var path string
	var serverlogLevel int
	var raftlogLevel int
	var isRestart bool
	flag.IntVar(&pid, "id", -1, "enter pid")
	flag.StringVar(&path,"path", "", "enter path")
	flag.IntVar(&serverlogLevel,"serverlogLevel", -1, "enter log level")
	flag.IntVar(&raftlogLevel,"raftlogLevel", -1, "enter log level")
	flag.BoolVar(&isRestart,"isRestart",false,"enter is restart Parameter")
	flag.Parse()
	//fmt.Printf("%v , %v , %v, %v , %v \n", pid, path, serverlogLevel,raftlogLevel,isRestart)
	
	if pid == -1 {
		fmt.Println("Invalid pid Arguments")
		os.Exit(0)
	}
	if path == "" {
		fmt.Println("Invalid path Arguments")
		os.Exit(0)
	}
	if serverlogLevel == -1 {
		fmt.Println("Invalid logLevel Arguments")
		os.Exit(0)
	}
	if raftlogLevel == -1 {
		fmt.Println("Invalid logLevel Arguments")
	}	
	
	// create server instance
	var s cluster.Server  
	var err error
	serverConfPath := path + "/conf/servers.json"
	s, err = cluster.New(pid, serverConfPath, nil, serverlogLevel)
	if err != nil {
		fmt.Println("Cound not instantiate server , Error : %v", err)
		return
	}
	
	var kvPath string
	kvPath = path + kvDir + "/" + kvName + strconv.Itoa(pid)
	// allocate log KV store
	if (isRestart == false) {
		err := db.DestroyDatabase(kvPath)
		if err != nil {
			log.Printf("Test : Cound not destroy previous KV the : %v ",kvPath )
			log.Printf("Test : Error %v ",err )
			s.Shutdown()
			return
		}
		conn := db.InitializeNewConnection()
		err = conn.OpenConnection(kvPath)
		if err != nil {
			log.Printf("Test : Cound not create connection : %v",kvPath )
			s.Shutdown()
			return
		} 
		conn.Close()
		if LOG_LEVEL > 0{	
			log.Println("Allocated new log data base")
		}
	}
	
	// create instance
	raftConfPath := path + "/conf"
	rObj, ok, err := raft.NewRaft(pid, raftConfPath, raftlogLevel, &s, kvPath ,isRestart)
	if ok == false {
			log.Println("Error Occured : Can in instantiate Raft Instances")
			s.Shutdown()
			return
	}
	if err != nil {
		log.Println(err)
		fmt.Println("errorr occured")
		s.Shutdown()
		return
	}
	time.Sleep(2*time.Second)	
	
	// connect with testCluster
	var testClusterObj cluster.Server
	
	// start debuging channel	
	testConfPath := path +  "/conf/TestConfig.json"
	testClusterObj, err = cluster.New(pid, testConfPath ,nil,0)

	LOOP:
		for {
			select {
			case env := <-testClusterObj.Inbox():
				if raft.LOG >= raft.HIGH {
					log.Printf("RAFT:The Test server GOT message  %v\n", pid)
					log.Printf("RAFT %v : %v\n", pid, env)
				}
				msg := env.Msg
				if msg.MsgCode == raft.CRequest {
					cmd ,ok := (msg.Msg).(raft.Command)
					if ok == false {
						log.Printf("raft %v : Command is expected", pid)
					} 
					if cmd.Cmd == raft.IsLeader {
						r := rObj.IsLeader()
						var lItem  *raft.LogItem
						if r == true {
							lItem =	getNewLogItem(1,nil)
						} else {
							lItem = getNewLogItem(-1,nil)
						}
						sendReply(env.Pid,lItem,testClusterObj)
					} else if cmd.Cmd == raft.Term {
						t := rObj.Term()
						var lItem  *raft.LogItem
						lItem =	getNewLogItem(-4,t)
						sendReply(env.Pid,lItem,testClusterObj)
					} else if cmd.Cmd == raft.Get {
						rObj.Outbox() <- &cmd
						res := <-rObj.Inbox()
						sendReply(env.Pid,res,testClusterObj)
					} else if cmd.Cmd == raft.Put {
						rObj.Outbox() <- &cmd
						res := <-rObj.Inbox()
						sendReply(env.Pid,res,testClusterObj)
					} else if cmd.Cmd == raft.Del {
						rObj.Outbox() <- &cmd
						res := <-rObj.Inbox()
						sendReply(env.Pid,res,testClusterObj)
					} else if cmd.Cmd == raft.LastLogTermAndIndex  {
						 index, term := rObj.LastLogIndexAndTerm()
						t := raft.IndexTerm{ Index: index , Term : term}
						res	:= raft.LogItem{Index: -5, Data: t } 
						sendReply(env.Pid,&res,testClusterObj)
					} else {
						if raft.LOG >= raft.HIGH {
							log.Printf("raft %v : Command Not supported ",pid)
						}
					} 
				}
				if msg.MsgCode == raft.SHUTDOWNRAFT {
					//fmt.Printf("RAFT:The Test gott Shutdown request  %v\n", f)
					rObj.Shutdown()
					s.Shutdown()
					time.Sleep(1 * time.Second)
					testClusterObj.Shutdown()
					break LOOP
				}
			}
	}
	testClusterObj.Shutdown()
	rObj.Shutdown()
	s.Shutdown()	
}

func getNewLogItem(index int64, data interface{}) (*raft.LogItem) {
	item :=  raft.LogItem{Index : index, Data: data}
	return &item
}

func sendReply(id int, lItem *raft.LogItem , testCluster cluster.Server) {
	replyMsg := cluster.Message{MsgCode: raft.CResponse, Msg: &lItem}
	replyEnv := cluster.Envelope{Pid: id, MsgType: cluster.CTRL, Msg: replyMsg}
	testCluster.Outbox() <- &replyEnv
}