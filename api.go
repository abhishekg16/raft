package raft

import "time"

type Raft interface {
	Term() int      // return the current term
	IsLeader() bool // return whether the current raft instance is Leader
	Pid() int       // return the Pid id current Raft instance
	Outbox() chan <- interface{} // Return a out channel on which client can send commands
	Inbox() <- chan *LogItem //Return In channel from where client can read the output for raft 
	Delay(time.Duration) // introduce delay in a leader
	Shutdown()           // Shutdown the raft instance
	
	// DEBUG
	LastLogIndexAndTerm() (int64,int64) // Return the lastLogTerm and Index
}
