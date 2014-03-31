package raft

import "testing"
import cluster "github.com/abhishekg16/cluster"
import "log"
import "time"

const (
	NOFSERVER        = 3
	NOFRAFT          = 3
	SERVER_LOG_LEVEL = INFO
	RAFT_LOG_LEVEL   = FINE
)

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

func makeRaftInstances(num int, s []cluster.Server) ([]*consensus, bool, error) {
	r := make([]*consensus, num)
	var ok bool
	var err error
	for i := 0; i < num; i++ {
		r[i], ok, err = NewRaft(i, "./conf", RAFT_LOG_LEVEL, &s[i], false)
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

/*
func TestRaft_SingleLeaderInATerm(t *testing.T) {
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
	ok, _ = checkLeaderShip(rObj)

	if ok == false {
		t.Errorf("Multiple Leaders \n ")
	}

	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)
}

*/


// This will few commands and will check whether they are successfully replicated
/*
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
	// send a commond on outbox of every one
	// only leader will accept and
	
	rObj[0].Outbox() <- "Add"
	reply := <-rObj[0].Inbox()
	log.Printf("reply %v\n", reply)
	
	rObj[1].Outbox() <- "Add"
	reply = <-rObj[1].Inbox()
	log.Printf("reply %v\n", reply)
	
	rObj[2].Outbox() <- "Add"
	reply = <-rObj[2].Inbox()
	log.Printf("reply %v\n", reply)
	
	shutdownServer(NOFSERVER, sObjs)
	shutdownRaft(NOFSERVER, rObj)
}
*/

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