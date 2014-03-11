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
)

func init() {
	gob.Register(raft.AppendEntriesToken{})
}

// This main function take raft instance Id as command line argument

func main() {
	var f int
	flag.IntVar(&f, "id", -1, "enter pid")
	flag.Parse()
	if f == -1 {
		fmt.Println("Invalid Arguments")
		os.Exit(0)
	}
	// create instance
	rObj, _, err := raft.NewRaft(f, "./conf", 0)
	if err != nil {
		log.Println(err)
		fmt.Println("errorr occured")
		os.Exit(1)
	}

	testCluster, err := cluster.New(f, "./conf/TestConfig.json",nil)
	if err != nil {
		fmt.Printf("RAFT:The Test server is not stated at %v\n", f)
		fmt.Println(err)
	}
	//fmt.Printf("RAFT:The Test server stated at %v\n", f)
LOOP:
	for {
		select {
		case env := <-testCluster.Inbox():
			//fmt.Printf("RAFT:The Test server GOT message  %v\n", f)
			//fmt.Printf("RAFT %v : %v\n", f, env)
			msg := env.Msg
			if msg.MsgCode == raft.HEARTBEAT {
				lead := rObj.IsLeader()

				cLeader := -1
				if lead == true {
					cLeader = rObj.Pid()
				}
				term := rObj.Term()
				replyToken := raft.AppendEntriesToken{Term: term, LeaderId: cLeader}
				replyMsg := cluster.Message{MsgCode: raft.HEARTBEAT, Msg: replyToken}
				replyEnv := cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: replyMsg}
				//fmt.Println("Sending Back %v", env)
				testCluster.Outbox() <- &replyEnv
			}
			if msg.MsgCode == raft.SHUTDOWNRAFT {
				//fmt.Printf("RAFT:The Test gott Shutdown request  %v\n", f)
				rObj.Shutdown()
				time.Sleep(1 * time.Second)
				testCluster.Shutdown()
				break LOOP
			}
		}
	}
	testCluster.Shutdown()
	//fmt.Printf("RAFT %v: OFF\n", f)

}
