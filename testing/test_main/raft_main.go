package main

import (
	cluster	"github.com/abhishekg16/cluster"
	raft "github.com/abhishekg16/raft"
	"encoding/gob"
	"fmt"
	"log"
	"flag"
	"os"
	"time"
) 

func  init() {
	gob.Register(raft.AppendEntriesToken{})
} 


func main() {
        var f int
        //fmt.Println("called")
        flag.IntVar(&f,"id",-1,"enter pid")
        flag.Parse()
        //fmt.Println(f)
        if f == -1 {
                fmt.Println("Invalid Arguments")
                os.Exit(0)
        }
        
        rObj, _ , err:= raft.NewRaft(f,"./conf",0)
        if err != nil {
                log.Println(err)
                fmt.Println("errorr occured")
                os.Exit(1)
        }
        /*
        log.Println("Main function: Raft Object instansiated")
        time.Sleep(3* time.Second)
        log.Println("main sunction called shutdown")
        rObj.Shutdown()
        time.Sleep(3* time.Second)
        log.Println("main sunction :exiting")
        os.Exit(0)
        */
        // start a cluster
        testCluster, err := cluster.New(f, "./conf/TestConfig.json")
        if err != nil {
        	fmt.Printf("RAFT:The Test server is not stated at %v\n",f)
        	fmt.Println(err)
        }
        fmt.Printf("RAFT:The Test server stated at %v\n",f)  
        LOOP:
        for {
        	select { 
        		case env := <-testCluster.Inbox():
        			fmt.Printf("RAFT:The Test server GOT message  %v\n",f)
        			fmt.Printf("RAFT %v : %v\n",f,env)
        			msg := env.Msg
        			if msg.MsgCode == raft.HEARTBEAT {
	        			lead	:= rObj.IsLeader()
	        			
	        			cLeader := -1
	        			if lead == true{
	        				cLeader = rObj.Pid()
	        			}
        				term := rObj.Term()
        		 		replyToken := raft.AppendEntriesToken{Term:term, LeaderId: cLeader}
						replyMsg := cluster.Message{MsgCode : raft.HEARTBEAT, Msg : replyToken }
        				replyEnv  := cluster.Envelope { Pid: env.Pid, MsgType: cluster.CTRL, Msg: replyMsg }
        				fmt.Println("Sending Back %v", env )    
        				testCluster.Outbox() <- &replyEnv 
        			}
        			if msg.MsgCode == raft.SHUTDOWN {
	        				rObj.Shutdown()
	        				time.Sleep(1 * time.Second)
	        				testCluster.Shutdown()
	        				break LOOP				
        			}
        	}	
        }
        		
}