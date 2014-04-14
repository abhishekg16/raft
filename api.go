package raft

import "time"

type Raft interface {
	Term() int                  // return the current term
	IsLeader() bool             // return whether the current raft instance is Leader
	Pid() int                   // return the Pid id current Raft instance
	Outbox() chan<- interface{} // Return a out channel on which client can send commands
	Inbox() <-chan *LogItem     //Return In channel from where client can read the output for raft
	Delay(time.Duration)        // introduce delay in a leader
	Shutdown()                  // Shutdown the raft instance

	// DEBUG
	LastLogIndexAndTerm() (int64, int64) // Return the lastLogTerm and Index
	PrintLog()                           // Print the Log in logging File
}

// Loginging Structs. This is send back to client
//  index = -1 (Not Leader)
//  index = -2 (Error)

type LogItem struct {
	Index int64
	Data  interface{}
}

// The Log will contains the array of log entries.
type LogEntry struct {
	Term    int64
	Command interface{}
}


const (
	Get = iota
	Put = iota
	Del = iota
)

// error code
const (
	CMD_INPROGRESS = iota
	CMD_APPLIED = iota
	CMD_OUT_OF_ORDER = iota
)

type Command struct{
	CmdId int64
	Cmd int
	Key []byte
	Value [] byte
}

// Result replied  by the KVInterface
type Result struct{
	Error error 
	Value []byte
}



