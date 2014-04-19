package raft

import "testing"
import "os/exec"
import "log"
import "os"
import "time"
import "strconv"
import cluster "github.com/abhishekg16/cluster"

 
const (
	NOFSERVER = 3
	PATH      = "/home/hduser/go" // GOPATH
	currDir =  "/home/hduser/go/src/github.com/abhishekg16/raft"
	TESTPID   = 3
	TEST_LOG_LEVEL = NOLOG
)

func startRaft(ePath string, pid int, isRestart bool) *exec.Cmd {
	sPid := strconv.Itoa(pid)
	var cmd *exec.Cmd
	if (isRestart == false) {
		cmd = exec.Command(ePath, "-id", sPid, "-path", currDir , "-serverlogLevel", strconv.Itoa(INFO), "-raftlogLevel", strconv.Itoa(INFO) )
	} else {
		cmd = exec.Command(ePath, "-id", sPid, "-path", currDir , "-serverlogLevel", strconv.Itoa(INFO), "-raftlogLevel", strconv.Itoa(INFO) , "-isRestart","true" )
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout
	return cmd
}

// sendRequest passes a request to passes pid using the cluster
func sendRequest(pid int , cmd *Command, testCluster cluster.Server ) {
	msg := cluster.Message{MsgCode: CRequest, Msg : *cmd}	
	env := cluster.Envelope{Pid: pid, MsgType: cluster.CTRL, Msg: msg}
	testCluster.Outbox() <- &env
}

// askLeader send IsLeader Request
func askLeader(testCluster cluster.Server, pid int) bool {
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Sending IsLeader Request to %v",pid)
	}
	cmd := Command {Cmd : IsLeader}
	sendRequest(pid,&cmd,testCluster)
	env := <-testCluster.Inbox()
	msg := (env.Msg.Msg).(LogItem)
	if msg.Index == -1 {
		return false
	} else {
		return true
	}
}
// askTerm send a Term request to pid
func askTerm(testCluster cluster.Server, pid int) int64 {
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Sending Term Request to %v",pid)
	}
	cmd := Command {Cmd : Term}
	sendRequest(pid,&cmd,testCluster)
	env := <-testCluster.Inbox()
	msg := (env.Msg.Msg).(LogItem) 
	return (msg.Data).(int64)
}

// putRequest send a put request  
func putRequest(key string , value string , testCluster cluster.Server, pid int) LogItem{
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Sending Put Request to %v",pid)
	}
	cmd := Command {Cmd: Put, Key : []byte(key) , Value:[]byte(value)}
	sendRequest(pid,&cmd,testCluster)
	env := <-testCluster.Inbox()
	return (env.Msg.Msg).(LogItem) 
}

// getRequest send a put request
func getRequest(key string , testCluster cluster.Server, pid int) string {
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Sending Get Request to %v",pid)
	}
	cmd := Command {Cmd: Get, Key : []byte(key)}
	sendRequest(pid,&cmd,testCluster)
	env := <-testCluster.Inbox()
	msg := (env.Msg.Msg).(LogItem)
	if msg.Index > 0 {
		v := (msg.Data).(Result)
		val := string(v.Value)
		return val
	}
	return ""
} 

// getIndex send a put request
func getIndexTerm(testCluster cluster.Server, pid int) (int64,int64) {
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Sending TermIndex Request to %v",pid)
	}
	cmd := Command {Cmd: LastLogTermAndIndex}
	sendRequest(pid,&cmd,testCluster)
	env := <-testCluster.Inbox()
	msg := (env.Msg.Msg).(LogItem)
	d := (msg.Data).(IndexTerm) 
	term := d.Term
	index := d.Index
	return index,term
}

// request Shutdown on the pid
func sendShutdown(testCluster cluster.Server, pid int) {
	msg := cluster.Message{MsgCode: SHUTDOWNRAFT}
	env := cluster.Envelope{Pid: pid, MsgType: cluster.CTRL, Msg: msg}
	testCluster.Outbox() <- &env
}



func isKilled(killed []int, pid int) bool{
	for i := 0 ; i < len(killed) ; i++ {
		if killed[i] == pid {
			return true
		}
	}
	return false
}

func findLeader(testCluster cluster.Server, killed []int) int{
	peers := testCluster.Peers()
	term := int64(0)
	leader := -1
	for {
		for i := 0 ; i < len(peers) ; i++ {
			peerId := peers[i]
			if isKilled(killed,peerId) == true {
				continue
			}
			flag := askLeader(testCluster,peerId) 
			if flag == true {
				if askTerm(testCluster,i) > term {
					leader = peerId
				}  
			}
		} 
		if leader != -1 {
			return leader
		} 
		if TEST_LOG_LEVEL >= HIGH  {
			log.Printf("Leader Node elected... Waiting...")
		}
		time.Sleep( 2 * time.Second )
	}
}


// Test there should be only one leader
func TestRaft_TS1(t *testing.T) {

	// path of the executable
	ePath := PATH + "/bin/test_main"
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Creating .. %v raft instances\n", NOFSERVER)
	}
	rInst := make([]*exec.Cmd, NOFSERVER)
	for i := 0; i < NOFSERVER; i++ {
		rInst[i] = startRaft(ePath, i,false)
	}
	// start server
	for i := 0; i < NOFSERVER; i++ {
		err := rInst[i].Start()
		if err != nil {
			log.Printf("Not able to start %v", i)
			log.Println(err)
		}
	}
	// introduce the wait to sattle down
	time.Sleep(3 * time.Second)

	// refer to testCluster
	var testClusterObj cluster.Server
	
	// start debuging channel	
	serverConfPath := currDir + "/conf/TestConfig.json"
	testClusterObj, err := cluster.New(TESTPID, serverConfPath ,nil,0)
	if err != nil {
		for i := 0; i < NOFSERVER; i++ {
			log.Println("Killing %v",i)
			rInst[i].Process.Kill()
		}
		log.Println("Error : Test Server Not started")
	}
	
	killed := make ([]int,0)

	// wait to test cluster sattle 
	time.Sleep(3 * time.Second)
	
	leader := findLeader(testClusterObj,killed)
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Current Leader is %v",leader)
	}
	j := 0
	// Put data in leader
	for i := 0 ; i < 3 ; i++ {
		key := "key" +  strconv.Itoa(j)
		value := "value" + strconv.Itoa(j)
		j++
		reply := putRequest(key, value,testClusterObj,leader)
		if TEST_LOG_LEVEL >= HIGH {
			log.Printf("reply : %v \n",reply)
		}
	}
	
	j--;
	key := "key" +  strconv.Itoa(j)
	value := "value" + strconv.Itoa(j)
	val := getRequest(key,testClusterObj,leader)
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("reply : %v \n",val)
	}
	if val != value {
		for i := 0; i < NOFSERVER; i++ {
		rInst[i].Process.Kill()	
		}
		testClusterObj.Shutdown()
		t.Errorf("Put-Get Test failed")
	} 
	
	
	// killing leader
	rInst[leader].Process.Kill()
	//rInst[leader] = nil
	killed = append(killed , leader) 
	
	time.Sleep(5 * time.Second)
	
	leader = findLeader(testClusterObj,killed)
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Current Leader is %v",leader)
	}
	
	
	// Put data in leader
	for i := 0 ; i < 3 ; i++ {
		key := "key" +  strconv.Itoa(j)
		value := "value" + strconv.Itoa(j)
		j++
		reply := putRequest(key, value,testClusterObj,leader)
		if TEST_LOG_LEVEL >= HIGH {
			log.Printf("reply : %v \n",reply)
		}
	}
	
	// Restart failed server 
	rInst[killed[0]] = startRaft(ePath, killed[0],true)
	err = rInst[killed[0]].Start()
	if err != nil {
		log.Printf("Not able to start %v", killed[0])
		log.Println(err)
	}
	// allow some time for all server to sattle
	time.Sleep(10 * time.Second)
	
	// verify all logs 
	indexes := make([]int64, 0)
	terms := make([]int64, 0)
	for i := 0; i < NOFSERVER; i++ {
		index, term := getIndexTerm(testClusterObj,i)
		indexes = append(indexes, index)
		terms = append(terms, term)
		if TEST_LOG_LEVEL >= HIGH {
			log.Printf("Raft %v: Index : %v , Term : %v ",i,index,term)
		}
	}
	
	for i := 0; i < NOFSERVER; i++ {
		//log.Println("Killing %v",i)
		rInst[i].Process.Kill()
	}
	
	testClusterObj.Shutdown()
	
	for i := 0; i < len(indexes)-1; i++ {
		if indexes[i] != indexes[i+1] || terms[i] != terms[i+1] {
			t.Errorf("The Log is not same on all the servers")
		}
	}
}
