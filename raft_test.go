package raft

import "testing"
import "os/exec"
//import "log"
//import "os"
import "time"
import "strconv"
import cluster "github.com/abhishekg16/cluster"
import "encoding/gob"
//import "fmt"

const (
	NOFSERVER = 3
	PATH      = "/home/hduser/go" // GOPATH
	TESTPID   = 3
	eTimeout  = 2
	numOfTest = 5 // number of time this it will request for current term and the leaderId
)

func startServer(ePath string, pid int) *exec.Cmd {
	sPid := strconv.Itoa(pid)
	cmd := exec.Command(ePath, "-id", sPid)
	//cmd.Stdout = os.Stdout
	//cmd.Stderr = os.Stdout
	return cmd
}

func init() {
	//fmt.Println("Init executed")
	gob.Register(AppendEntriesToken{})
}

func askForLeader(testCluster cluster.Server) {
	//fmt.Println("Sending Leader Request")
	msg := cluster.Message{MsgCode: HEARTBEAT}
	env := cluster.Envelope{Pid: cluster.BROADCAST, MsgType: cluster.CTRL, Msg: msg}
	testCluster.Outbox() <- &env
}

func sendShutdown(testCluster cluster.Server, pid int) {
	//fmt.Println("Sending Shutdown Request")
	msg := cluster.Message{MsgCode: SHUTDOWNRAFT}
	env := cluster.Envelope{Pid: pid, MsgType: cluster.CTRL, Msg: msg}
	testCluster.Outbox() <- &env
}

type peerResponse struct {
	Term   int64
	Leader int
}

// getIndexOfPeer return the index of the peer. If peer does not exist the it reply -1
func getIndexOfPeer(pid int, peers []int) int {
	for i := 0; i < len(peers); i++ {
		if peers[i] == pid {
			return i
		}
	}
	return -1
}

// waitForLeader wiats for the response from all the running clusters and
// very that there should be only one leader in a term
func waitForLeader(testCluster cluster.Server, waitChan chan bool) {
	//fmt.Println("Waiting Leader Request")
	peers := testCluster.Peers()     // list of all the servers
	peerStatus := make(map[int]bool) // peerStatus keeps an entry for a server with whihch it is able to contact
	// initially mark each server as dead
	for pid := range peers {
		peerStatus[pid] = false
	}

	//log.Printf("No of peers %v \n", len(peers))
	peerResponse := make([]int, len(peers))

	
	// mark all entries as -1 - an invalid id

	for i := 0; i < len(peers); i++ {
			peerResponse[i] = -2
	}

	askForLeader(testCluster)
	time.Sleep(2 * time.Second)
	attempt := 0
	LOOP:
	for {
		attempt++
		select {
		case env := <-testCluster.Inbox():
		//	fmt.Println("Got Response....The Raft Test Client")
		//	fmt.Println("Received Response %v", env)
			msg, ok := ((env.Msg).Msg).(AppendEntriesToken)
			if ok == false {
				//fmt.Println("Count not type cast ")
				continue
			}
			
			peerStatus[env.Pid] = true
			index := getIndexOfPeer(env.Pid, peers)
			peerResponse[index] = msg.LeaderId
		case <-time.After(( 1 * eTimeout) * time.Second):
			break LOOP
		}
	}
	//log.Printf("Peer Response %v", peerResponse)
	
	
		count := 0
		for i := 0; i < len(peers); i++ {
			if peerResponse[i] != -1 && peerResponse[i] != -2 {
				count++
			}
		}

		//log.Println("count ",count)
		if count == 1 { 
			waitChan <- true
		}else { 
			waitChan <- false
		}
	
}

// Test there should be only one leader
func TestRaft_TS1(t *testing.T) {

	// path of the executable
	ePath := PATH + "/bin/test_main"
	//log.Println(ePath)
	//log.Printf("Creating .. %v raft instances\n", NOFSERVER)
	rInst := make([]*exec.Cmd, NOFSERVER)
	for i := 0; i < NOFSERVER; i++ {
		rInst[i] = startServer(ePath, i)
	}

	// starting  raft processes
	for i := 0; i < NOFSERVER; i++ {
		err := rInst[i].Start()
		if err != nil {
		//	log.Printf("Not able to start %v", i)
		//	log.Println(err)

		}
	}

	// start debuging channel
	testCluster, err := cluster.New(TESTPID, "./conf/TestConfig.json",nil)
	if err != nil {
	//	log.Println("TestCluster start Failed")
	}
	testClusterObj := testCluster
	time.Sleep(3 * eTimeout * time.Second)


	// wait channel which will be used to get back the response form the waitForLeader Method
	waitChan := make(chan bool, 10)
	go waitForLeader(testClusterObj, waitChan)
	//askForLeader(testClusterObj)

	ok := <-waitChan

	if ok == false {
		t.Errorf("Multiple Servers")
	}

	//log.Println("killing Raft instances")

	for i := 0; i < NOFSERVER; i++ {
		sendShutdown(testClusterObj,i)
		//rInst[i].Process.Kill()
	}
	time.After(3 * time.Second)
	testCluster.Shutdown()
	
}



/*
// kill the leader
func TestRaft_TS2(t *testing.T) {
	// path of the executable
	ePath := PATH + "/bin/test_main"
	log.Println(ePath)
	log.Printf("Creating .. %v raft instances\n", NOFSERVER)
	rInst := make([]*exec.Cmd, NOFSERVER)
	for i := 0; i < NOFSERVER; i++ {
		rInst[i] = startServer(ePath, i)
	}

	// starting  raft processes
	for i := 0; i < NOFSERVER; i++ {
		err := rInst[i].Start()
		if err != nil {
			log.Printf("Not able to start %v", i)
			log.Println(err)
		}
	}

	// waiting for server to stablize
	time.Sleep(eTimeout * time.Second)

	// start debuging channel
	testCluster, err := cluster.New(TESTPID, "./conf/TestConfig.json")
	if err != nil {
		log.Println("TestCluster start Failed")
	}
	testClusterObj := testCluster
	time.Sleep(5 * time.Second)

	// wait channel which will be used to get back the response form the waitForLeader Method
	waitChan := make(chan bool, 10)
	go waitForLeader(testClusterObj, waitChan)

	ok := <-waitChan

	if ok == false {
		t.Errorf("Multiple Servers")
	}

	log.Println("Stoping Raft instances")

	for i := 0; i < NOFSERVER; i++ {
		sendShutdown(testClusterObj)
		//rInst[i].Process.Kill()
	}
	// shutdown test cluster
	testCluster.Shutdown()
}
*/
