package raft

import "testing"
import cluster "github.com/abhishekg16/cluster"
import "log"
import "time"
import db "github.com/abhishekg16/raft/dataBaseConnection"
import "strconv"

const (
	NOFSERVER        = 3
	NOFRAFT          = 3
	SERVER_LOG_LEVEL = NOLOG
	RAFT_LOG_LEVEL   = NOLOG
	TEST_LOG_LEVEL = NOLOG
	kvDir = "./kvstore"
	kvName = "kv" 
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func makeDummyServer(num int) ([]cluster.Server, error) {
	s := make([]cluster.Server, num)
	var err error
	for i := 0; i < num; i++ {
		s[i], err = cluster.New(i, "./conf/servers.json", nil, SERVER_LOG_LEVEL)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}
// this method make keyValue store, name is appended by the num
func makeKVStores(num int)  error{
	// destroy the previous databases
	 
	for i := 0 ; i < num ; i++ {
		path := kvDir + "/" + kvName + strconv.Itoa(i)
		err := db.DestroyDatabase(path)
		if err != nil {
			log.Printf("Test : Cound not destroy previous KV the : %v",path )
			return err 
		}
		conn := db.InitializeNewConnection()
		err = conn.OpenConnection(path)
		if err != nil {
			log.Printf("Test : Cound not create connection : %v",path )
		} 
		conn.Close()
	}
	return nil
}

func makeRaftInstances(num int, s []cluster.Server, ) ([]*consensus, bool, error) {
	r := make([]*consensus, num)
	var ok bool
	var err error
	for i := 0; i < num; i++ {
		path := kvDir + "/" + kvName + strconv.Itoa(i)
		r[i], ok, err = NewRaft(i, "./conf", RAFT_LOG_LEVEL, &s[i], path ,false)
		if ok == false {
			log.Println("Error Occured : Can in instantiate Raft Instances")
			return r, false, err
		}
	}
	return r, true, nil
}

func checkLeaderShip(rObj []*consensus) (bool, int64) {
	leaderCount := make(map[int64]int, 0)
	for i := 0; i < NOFRAFT; i++ {
		term := rObj[i].Term()
		count := 0
		count, _ = leaderCount[term]
		if rObj[i].IsLeader() == true {
			count = count + 1
		}
		leaderCount[term] = count
	}
	//log.Println(leaderCount)
	highestT := int64(0)
	for k, v := range leaderCount {
		if k > highestT {
			highestT = k
		}
		if v > 1 {
			return false, highestT
		}
	}
	return true, highestT

}

func getLeaderInTerm(num int, t int64, r []*consensus) int {
	for i := 0; i < num; i++ {
		term := r[i].Term()
		if term == t {
			if r[i].IsLeader() {
				return i
			}
		}
	}
	return -1
}

func shutdownServer(num int, s []cluster.Server) {
	for i := 0; i < num; i++ {
		s[i].Shutdown()
	}
}

func shutdownRaft(num int, r []*consensus) {
	for i := 0; i < num; i++ {
		r[i].Shutdown()
	}
}

func findLeader( rObj []*consensus) int{
	term := int64(0)
	leader := -1
	for {
		for i := 0 ; i < NOFRAFT ; i++ {
			if (rObj[i] == nil) {
				continue
			}
			if rObj[i].IsLeader() == true {
				if rObj[i].Term() > term {
					leader = i
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



func TestRaft_SingleLeaderInATerm(t *testing.T) {
	sObjs, err := makeDummyServer(NOFSERVER)
	if err != nil {
		log.Println(err)
		t.Errorf("Cound not instantiate server instances")
	}
	makeKVStores(NOFRAFT)
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		t.Errorf("Cound not instantiate Raft Instance instances")
	}

	time.Sleep(3 * time.Second)
	ok, _ = checkLeaderShip(rObj)

	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)
	if ok == false {
		t.Errorf("Multiple Leaders \n ")
	}

}


func TestRaft_PutAndGetTest(t *testing.T) {
	sObjs, err := makeDummyServer(NOFSERVER)
	
	if err != nil {
		log.Println(err)
		t.Errorf("Cound not instantiate server instances")
	}
	
	makeKVStores(NOFRAFT)
	
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		t.Errorf("Cound not instantiate Raft Instance instances")
	}
	time.Sleep(3* time.Second)
	
	cmd1 := Command{Cmd : Put , Key : []byte("Key1")  , Value: []byte("value1")}

	var reply *LogItem
	
	leader := 0
	
	LOOP1:
	for {
		rObj[leader].Outbox() <- &cmd1
		select {
			case reply = <-rObj[leader].Inbox():
				//
			case <-time.After(2 * time.Second):
		//		log.Printf("Resending to  : %v",leader)
				continue
		}
		if reply.Index == -1 {
	//		log.Println("reply : %v",reply)
			leader = reply.Data.(int)
		} else {
	//		log.Printf("Reply : %v",reply)
			break LOOP1
		}
	}
	
	cmd1 = Command{Cmd : Get , Key : []byte("Key1")  ,}
	leader = 0
	LOOP2:
	for {
		rObj[leader].Outbox() <- &cmd1
		select {
			case reply = <-rObj[leader].Inbox():
				//
			case <-time.After(2 * time.Second):
		//		log.Printf("Resending to  : %v",leader)
				continue
		}
		if reply.Index == -1 {
	//		log.Println("reply : %v",reply)
			leader = reply.Data.(int)
		} else {
	//			log.Printf("Reply : %v",reply)
			break LOOP2
		}
	}
	
	value := (reply.Data).(Result)
	val := string(value.Value)

	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)

	
	if val  != "value1" {
		t.Errorf("All machine did not reponded ")
	}
	
}



/*
// This will few commands and will check whether they are successfully replicated

// This test willl check weather servers are handling request properly
// leader should handle request and other should resturn the leaderID
func TestRaft_SingleCommandTest(t *testing.T) {
	sObjs, err := makeDummyServer(NOFSERVER)
	if err != nil {
		log.Println(err)
		t.Errorf("Cound not instantiate server instances")
	}
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		t.Errorf("Cound not instantiate Raft Instance instances")
	}

	time.Sleep(10 * time.Second)
	cmd1 := Command{Cmd : Put , Key : []byte("Key1")  , Value: []byte("value1")}
	// send a commond on outbox of every one
	// only leader will accept and
	// other should reply the current leader
	var reply *LogItem
	count := 0
	for i := 0; i < NOFRAFT; i++ {
		leader := i
	LOOP1:
		for {
			rObj[leader].Outbox() <- &cmd1
			select {
			case reply = <-rObj[leader].Inbox():
				//
			case <-time.After(3 * time.Second):
				if TEST_LOG_LEVEL >= HIGH {
//					log.Printf("Resending to  : %v",leader)
				}
				continue
			}
			if reply.Index == -1 {
				if TEST_LOG_LEVEL >= HIGH {
		//			log.Println("reply : %v",reply)
				}
				leader = reply.Data.(int)
			} else {
				count++
				if TEST_LOG_LEVEL >= HIGH {
	//				log.Printf("Reply : %v",reply)
				}
				break LOOP1
			}
		}
	}
	for i := 0 ; i < NOFRAFT ; i++{
		rObj[i].PrintLog()
	}
	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)

	if count != 3 {
		t.Errorf("All machine did not reponded ")
	}
}
*/
// This test check
// 1. Stop the leader case (delayed leader)
// 2. All the server must have same log


func TestRaft_MultipleCommandTestWithDelay(t *testing.T) {

	time.Sleep(2*time.Second)
	sObjs, err := makeDummyServer(NOFSERVER)
	if err != nil {
		log.Println(err)
		t.Errorf("Cound not instantiate server instances")
	}
	
	makeKVStores(NOFRAFT)
	
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		t.Errorf("Cound not instantiate Raft Instance instances")
	}

	

	time.Sleep(2 * time.Second)
	// send a commond on outbox of every one
	// only leader will accept and

	leader := 0

	cmd1 := Command{Cmd : Put , Key : []byte("Key1")  , Value: []byte("value1")}
	for i := 0; i < 5; i++ {
	LOOP1:
		for {
			//		log.Printf("Leader : %v",leader)
			rObj[leader].Outbox() <- &cmd1
			reply := <-rObj[leader].Inbox()
			if reply.Index == -1 {
				leader = reply.Data.(int)
			} else {
				//		log.Printf("Reply : %v",reply)
				break LOOP1
			}
		}
	}

	// introduce delay
	rObj[leader].Delay(10 * time.Second)
	var reply *LogItem
	for i := 0; i < 5; i++ {
	LOOP2:
		for {
			//log.Printf("Leader : %v",leader)
			rObj[leader].Outbox() <- &cmd1
			select {
				case reply = <-rObj[leader].Inbox():
					if TEST_LOG_LEVEL >= HIGH { 
						log.Printf("Reply : %v",reply)
					}
				case <-time.After(2 * time.Second):
					{
						if TEST_LOG_LEVEL >= HIGH { 
							log.Printf("Resending to  : %v",leader)
						}
						continue
					}
			}
			if reply.Index == -1 {
				newleader := reply.Data.(int)
				if (newleader == leader) {
					// no leader at present
					time.Sleep(3*time.	Second)
				}	
				leader = newleader
			} else {
				break LOOP2
			}
		}
	}
	time.Sleep(3 * time.Second)

	indexes := make([]int64, 0)
	terms := make([]int64, 0)
	for i := 0; i < NOFRAFT; i++ {
		index, term := rObj[i].LastLogIndexAndTerm()
		indexes = append(indexes, index)
		terms = append(terms, term)
		//log.Printf("Raft %v: Index : %v , Term : %v ",i,index,term)
		rObj[i].PrintLog()
	}

	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)

	for i := 0; i < len(indexes)-1; i++ {
		if indexes[i] != indexes[i+1] || terms[i] != terms[i+1] {
			t.Errorf("The Log is not same on all the servers")
		}

	}
}





//	This method send few command and then restartLeader, mean while system proceed with new command on the new leader.
//	Expected behavious : the restarted machine should get in sync with other machine and eventaully must have same log



func TestRaft_RestartLeader(t *testing.T) {

	time.Sleep(1*time.Second)
	sObjs, err := makeDummyServer(NOFSERVER)
	if err != nil {
		log.Println(err)
		t.Errorf("Cound not instantiate server instances")
	}
	
	makeKVStores(NOFRAFT)
	
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		t.Errorf("Cound not instantiate Raft Instance instances")
	}

	// inserted delay to allow system to stabalize
	time.Sleep(2 * time.Second)
	
	leader := 0
	j := 0
	
	leader = findLeader(rObj)
	
	
	if TEST_LOG_LEVEL >= HIGH  {
		log.Printf("Leader is %v ",leader)
	}	
		

	for i := 0; i < 3; i++ {
		if TEST_LOG_LEVEL >= HIGH  {			
			log.Printf("Sending to Leader : %v",leader)
		}
		key := "key" +  strconv.Itoa(j)
		value := "value" + strconv.Itoa(j)
		j++
		cmd1 := Command{Cmd : Put , Key : []byte(key)  , Value: []byte(value)}
		rObj[leader].Outbox() <- &cmd1
		reply := <-rObj[leader].Inbox()
		if TEST_LOG_LEVEL >= HIGH {
			log.Printf("Got Reply %+v", reply)
		}
	}

	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Shuting down the leader...")
	}
	s_stopped := leader
	sObjs[leader].Shutdown()
	rObj[leader].Shutdown()
	sObjs[leader] = nil
	rObj[leader] = nil
	time.Sleep(3*time.Second)
	
	leader = findLeader(rObj)
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("New Leader is %v", leader)
	}
	
	for i := 0; i < 2; i++ {
		if TEST_LOG_LEVEL >= HIGH  {  			
			log.Printf("Sending to Leader : %v",leader)
		}
		key := "key" +  strconv.Itoa(j)
		value := "value" + strconv.Itoa(j)
		j++
		cmd1 := Command{Cmd : Put , Key : []byte(key)  , Value: []byte(value)}
		rObj[leader].Outbox() <- &cmd1
		reply := <-rObj[leader].Inbox()
		if TEST_LOG_LEVEL >= HIGH {
			log.Printf("Got Reply %v", reply)
		}
	}
	
	
	// restart the stoped server
	sObjs[s_stopped], err = cluster.New(s_stopped, "./conf/servers.json", nil, SERVER_LOG_LEVEL)
		path := kvDir + "/" + kvName + strconv.Itoa(s_stopped)
	rObj[s_stopped], ok, err = NewRaft(s_stopped, "./conf", RAFT_LOG_LEVEL, &sObjs[s_stopped], path ,true)
	if ok == false {
		log.Println("Error Occured : Can in instantiate Raft Instances")
		t.Errorf("Could not instantiate new Instance")
	}
	
	// wait for system to sync 
	time.Sleep(3 * time.Second)

	// verify the logs
	indexes := make([]int64, 0)
	terms := make([]int64, 0)
	for i := 0; i < NOFRAFT; i++ {
		index, term := rObj[i].LastLogIndexAndTerm()
		indexes = append(indexes, index)
		terms = append(terms, term)
		//log.Printf("Raft %v: Index : %v , Term : %v ",i,index,term)
		if TEST_LOG_LEVEL >= HIGH{
			rObj[i].PrintLog()
		}
	}

	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)

	for i := 0; i < len(indexes)-1; i++ {
		if indexes[i] != indexes[i+1] || terms[i] != terms[i+1] {
			t.Errorf("The Log is not same on all the servers")
		}

	}
}



//	This method check the idempotent prperty. 
//	Test Case : Client send same message multiple time. But system must execute that only once


func TestRaft_Idempotent(t *testing.T) {

	time.Sleep(1*time.Second)
	sObjs, err := makeDummyServer(NOFSERVER)
	if err != nil {
		log.Println(err)
		t.Errorf("Cound not instantiate server instances")
	}
	
	makeKVStores(NOFRAFT)
	
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		t.Errorf("Cound not instantiate Raft Instance instances")
	}

	// inserted delay to allow system to stabalize
	time.Sleep(2 * time.Second)
	
	leader := 0
	j := 0
	
	leader = findLeader(rObj)
	
	if TEST_LOG_LEVEL >= HIGH  {
		log.Printf("Leader is %v ",leader)
	}	
		
	key := "key" +  strconv.Itoa(j)
	value := "value" + strconv.Itoa(j)
	j++
	cmd1 := Command{CmdId : 1, Cmd : Put , Key : []byte(key)  , Value: []byte(value)}

	for i := 0; i < 2; i++ {			
		if TEST_LOG_LEVEL >= HIGH {
			log.Printf("Sending to Leader : %v",leader)
		}		
		rObj[leader].Outbox() <- &cmd1
	}
	 
	reply1 := <-rObj[leader].Inbox()
	reply2 := <-rObj[leader].Inbox()

	
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf ("reply1 : %+v" ,reply1)
		log.Printf ("reply1 : %+v" ,reply2)
	}
	
	for i := 0 ; i < NOFRAFT ; i++ {
	//	rObj[i].PrintLog()
	}
	
	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)
	
	if reply1.Index != -2 {
		t.Errorf("Same Command Applieded multiple times")
	} 
}


func TestRaft_IntroducePartition(t *testing.T) {

	time.Sleep(1*time.Second)
	sObjs, err := makeDummyServer(NOFSERVER)
	if err != nil {
		log.Println(err)
		t.Errorf("Cound not instantiate server instances")
	}
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		t.Errorf("Cound not instantiate Raft Instance instances")
	}

	// inserted delay to allow system to stabalize
	time.Sleep(2 * time.Second)
	
	leader := 0
	j := 0
	
	leader = findLeader(rObj)
	
	
	if TEST_LOG_LEVEL >= HIGH  {
		log.Printf("Leader is %v ",leader)
	}	
		

	for i := 0; i < 3; i++ {
		if TEST_LOG_LEVEL >= HIGH  {			
			log.Printf("Sending to Leader : %v",leader)
		}
		key := "key" +  strconv.Itoa(j)
		value := "value" + strconv.Itoa(j)
		j++
		cmd1 := Command{Cmd : Put , Key : []byte(key)  , Value: []byte(value)}
		rObj[leader].Outbox() <- &cmd1
		reply := <-rObj[leader].Inbox()
		if TEST_LOG_LEVEL >= HIGH {
			log.Printf("Got Reply %+v", reply)
		}
	}

	partitionMap := make(map[int]int,NOFSERVER)
	
	for i := 0 ;  i < NOFRAFT ; i++ {
		if i == leader  {
			partitionMap[i] = 0		
		} else  {
			partitionMap[i] = 1
		}
	} 
	
	for i := 0 ; i < NOFSERVER ; i++ {
		sObjs[i].Partition(partitionMap)
	}
	
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Introduced Partitioning...")
	}
	
	time.Sleep(10*time.Second)
	
	leader = findLeader(rObj)
	
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("New Leader is %v", leader)
	}
	
	for i := 0; i < 2; i++ {
		if TEST_LOG_LEVEL >= HIGH  {  			
			log.Printf("Sending to Leader : %v",leader)
		}
		key := "key" +  strconv.Itoa(j)
		value := "value" + strconv.Itoa(j)
		j++
		cmd1 := Command{Cmd : Put , Key : []byte(key)  , Value: []byte(value)}
		rObj[leader].Outbox() <- &cmd1
		reply := <-rObj[leader].Inbox()
		if TEST_LOG_LEVEL >= HIGH {
			log.Printf("Got Reply %v", reply)
		}
	}
	
	time.Sleep(3*time.Second)
	
	// remove partition
	for i := 0 ; i < NOFSERVER ; i++ {
		sObjs[i].RemovePartitions()
	}
	if TEST_LOG_LEVEL >= HIGH {
		log.Printf("Removed Partitioning...")
	}
	
		
	// wait for system to sync 
	time.Sleep(4 * time.Second)

	// verify the logs
	indexes := make([]int64, 0)
	terms := make([]int64, 0)
	for i := 0; i < NOFRAFT; i++ {
		index, term := rObj[i].LastLogIndexAndTerm()
		indexes = append(indexes, index)
		terms = append(terms, term)
		//log.Printf("Raft %v: Index : %v , Term : %v ",i,index,term)
		if TEST_LOG_LEVEL >= HIGH{
			rObj[i].PrintLog()
		}
	}

	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)

	for i := 0; i < len(indexes)-1; i++ {
		if indexes[i] != indexes[i+1] || terms[i] != terms[i+1] {
			t.Errorf("The Log is not same on all the servers")
		}

	}
}





func benchmarkRaft(num int,b * testing.B) {
	sObjs, err := makeDummyServer(NOFSERVER)
	if err != nil {
		log.Println(err)
		b.Errorf("Cound not instantiate server instances")
	}
	
	makeKVStores(NOFRAFT)
	
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		b.Errorf("Cound not instantiate Raft Instance instances")
	}

	// inserted delay to allow system to stabalize
	time.Sleep(1 * time.Second)
	
	leader := 0
	j := 0
	
	leader = findLeader(rObj)
	
    for i := 0; i < b.N; i++ {
    	key := "key" +  strconv.Itoa(j)
		value := "value" + strconv.Itoa(j)
		j++
		cmd1 := Command{Cmd : Put , Key : []byte(key)  , Value: []byte(value)}
		rObj[leader].Outbox() <- &cmd1
		reply := <-rObj[leader].Inbox()
		if TEST_LOG_LEVEL >= HIGH {
			log.Printf("Got Reply %v", reply)
		}       
    }
    
    shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)
}



func BenchmarkRaft1(b *testing.B)  { benchmarkRaft(100, b) }
func BenchmarkRaft2(b *testing.B)  { benchmarkRaft(1000, b) }
func BenchmarkRaft3(b *testing.B)  { benchmarkRaft(10000, b) }
func BenchmarkRaft4(b *testing.B) { benchmarkRaft(100000, b) }
func BenchmarkRaft5(b *testing.B) { benchmarkRaft(1000000, b) }



//func BenchmarkFib40(b *testing.B) { benchmarkFib(40, b) }





/*
func TestRaft_DelayIntroducedInLeader(t *testing.T) {

	sObjs, err := makeDummyServer(NOFSERVER)
	if err != nil {
		log.Println(err)
		t.Errorf("Cound not instantiate server instances")
	}
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		t.Errorf("Cound not instantiate Raft Instance instances")
	}
	time.Sleep(10 * time.Second)

	ok, term := checkLeaderShip(rObj)
	if ok == false {
		t.Errorf("Multiple Leaders \n ")
	}
	leader := getLeaderInTerm(NOFRAFT, term, rObj)
	if leader == -1 {
		t.Errorf("No Leader Present in term \n ")
	} else {
		rObj[leader].Delay(5 * time.Second)


		time.Sleep(15 * time.Second)
		ok, _ = checkLeaderShip(rObj)
		if ok == false {
			t.Errorf("Multiple Leaders \n ")
		}
	}
	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)
}

*/


/*
// This will few commands and will check whether they are successfully replicated
func TestRaft_MultipleCommondTest(t *testing.T) {
	sObjs, err := makeDummyServer(NOFSERVER)
	if err != nil {
		log.Println(err)
		t.Errorf("Cound not instantiate server instances")
	}
	rObj, ok, err := makeRaftInstances(NOFRAFT, sObjs)
	if ok == false {
		log.Println(err)
		t.Errorf("Cound not instantiate Raft Instance instances")
	}

	time.Sleep(10 * time.Second)
	// send a commond on outbox of every one
	// only leader will accept and

	// assuming leader is 1
	leader := 1

	for i := 0 ; i < 10 ; i++ {
		rObj[leader].Outbox() <- "Add"
		reply := <-rObj[leader].Inbox()
		log.Printf("reply %v\n", reply)
		if reply.Index == -1 {
			leader = reply.Data
		}
	}

	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)
}
*/
