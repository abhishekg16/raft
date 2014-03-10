package raft

import "time"

type Raft interface {
	Term() int      // return the current term
	IsLeader() bool // return whether the current raft instance is Leader
	Pid() int       // return the Pid id current Raft instance

	// Only available in DEBUG mode
	Block(duration time.Duration) bool // Block the raft instance for specific duration
	Resume() bool                      // Resume the blocked raft instance
	Shutdown()                         // Shutdown the raft instance
}
