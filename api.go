package raft

import "time"

type Raft interface {
	Term() int      // return the current term
	IsLeader() bool // return whether the current raft instance is Leader
	Pid() int       // return the Pid id current Raft instance
	Outbox() chan <- interface{}
	Inbox() <- chan *LogItem 

	Delay(time.Duration) // introduce delay in a leader
	Shutdown()           // Shutdown the raft instance
	
}
