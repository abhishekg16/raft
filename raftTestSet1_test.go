package raft 

import "testing"
cluster "github.com/abhishekg16/cluster"

const (
	NOFSERVER  = 1
	NOFRAFT  = 1
	SERVER_LOG_LEVEL = NOLOG
	RAFT_LOG_LEVEL = HIGH 
)

func makeDummyServer(num int) ([]*server, error) {
	s := make([]*server, num)
	var err error
	for i := 0; i < num; i++ {
		s[i], err = New(i, "./conf/Config.json",nil,SERVER_LOG_LEVEL)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}


func makeRaftInstanced(num int, []*server) ([]*consensus, ok., err ){
	r := make([]*consensus,num)
	for i := 0 ; i < num ; i++ {
		r[i], ok, err  = func NewRaft(i, "./conf", RAFT_LOG_LEVEL, s[i] ,false)
		if  ok == false {
			log.Println("Error Occured : Can in instantiate Raft Instances")
			return r, false , err 
		}
	}
	return r , true , nil
}


func TestRaft_TS1(t *testing.T) {
	sObjs := makeDummyServer(NOFSERVER)
	rObjs := makeRaftInstances(NOFRAFT)
	time.Sleep(20* time.Second )
}