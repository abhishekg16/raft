package main

import "os/exec"
import "log"
import "os"
import "time"
import "strconv"
import cluster	"github.com/abhishekg16/cluster"
import "encoding/gob"
import "fmt"

const (
	NOFSERVER = 3
	PATH = "/home/hduser/go"   // GOPATH
	TESTPID = 3
	eTimeout = 20  
)

type AppendEntriesToken  struct {
	Term int64
	LeaderId int
}


const (
        HEARTBEAT = iota
)
        
func startServer(ePath string, pid int)  *exec.Cmd {
	sPid := strconv.Itoa(pid)
	cmd :=  exec.Command(ePath, "-id", sPid)	
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout 
	return cmd
}


func init(){
		gob.Register(AppendEntriesToken{})
}

func askForLeader(testCluster cluster.Server)  { 
	fmt.Println("Sending Leader Request")
	msg := cluster.Message{MsgCode:HEARTBEAT}
	env := cluster.Envelope{Pid : cluster.BROADCAST , MsgType : cluster.CTRL, Msg: msg}
	testCluster.Outbox() <- &env	
} 

func waitForLeader(testCluster cluster.Server, waitChan chan bool){
	fmt.Println("Waiting Leader Request")
	peers := testCluster.Peers()
	peerStatus := make ([]bool, len(peers))
	for i:= 0 ; i < len(peers) ; i++  {
		peerStatus[i] = false 
	}
	peerRes := make ([]int, len(peers))
	termRes := make ([]int64, len(peers))
	askForLeader(testCluster)
	LOOP:
	for {
		select {
			case env := <- testCluster.Inbox():
				fmt.Println("Got Response....The Raft Test Client")
				fmt.Println("Received Response %v",env)
				msg, ok := ((env.Msg).Msg).(AppendEntriesToken)
				if ok == false {
					fmt.Println("Count not type cast ")
					continue
				} 
				
				peerStatus[env.Pid] = true
				peerRes[env.Pid] = msg.LeaderId
				termRes[env.Pid] = msg.Term 
			case <-time.After( eTimeout * time.Second ):
				askForLeader(testCluster)
				break LOOP 
		}
	}
	count := 0
	leader:= -1
	for i := 0 ; i < len(peers); i++  {
		 if peerStatus[i] == true {
		 	if peerRes[i] != -1 {
		 		leader = i 
		 		count++
		 	}
		}
	}
	fmt.Println("Wait for leader over") 
	if count == 1  {
		fmt.Println("Leader is %v",leader)
		waitChan <- true
	}
	waitChan <- false  

	
} 


//This example start a raft instane and wait till leader is elected 

func main() {
	ePath := PATH + "/bin/raft"  
	log.Println(ePath)
	
	log.Printf("Creating .. %v raft instances\n", NOFSERVER)
	
	rInst := make ([] * exec.Cmd, NOFSERVER)
		
	for i := 0 ; i < NOFSERVER ; i++ {
		rInst[i] = startServer(ePath, i)
	}
	
	// starting 
	for i := 0 ; i < NOFSERVER ; i++ {
		err :=	rInst[i].Start()
		if err != nil {
			log.Printf("Not able to start %v", i)
			log.Println(err)
			
		}	
	}
	
	// waiting for server to start
	
	
	// start Local cluster instance
	testCluster, err := cluster.New(TESTPID,"./conf/TestConfig.json")
	if err != nil {
		log.Println("TestCluster start Failed")
	}
	
	testClusterObj := testCluster;

 	
 	time.Sleep(5 * time.Second)
 	
 	waitChan := make (chan bool, 10)
 	
 	go waitForLeader(testClusterObj, waitChan)
 	
 	 
 	 ok := <- waitChan
 	
 	if ok == false {
 		
 		fmt.Println("Multiple Servers")
 	}	
 	
 	log.Println("killing Raft instances")
 	
 	for  i := 0 ; i < NOFSERVER ; i++ {
 		rInst[i].Process.Kill()	
 	} 
	log.Println("The process has started") 	
}
