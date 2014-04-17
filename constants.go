package raft

// This file contains all the constant used in the raft implementations
const (
	INFO    = iota
	HIGH    = iota
	WARNING = iota
	FINE    = iota
	FINEST  = iota
	NOLOG   = -1
)

// FILENAME is the name of the file which will store the persistent data
// related to the current raft instance

const (
	FILENAME    = "book"
	LOGFILENAME = "log"
	DATABASE_NAME = "db"
)

// These constant will identify the types of the Messages s
const (
	APPEND_ENTRY = iota
	LOGMESSAGE   = iota // Repesent the log  message
	VOTEREQUEST  = iota // Repesent the vote request message
	VOTERESPONSE = iota // Repesent the vote reponse message
	RESPONSE     = iota // This message is used for response of append entry
	SHUTDOWNRAFT = iota
)

// Differnet Possible States of the Raft System
const (
	LEADER    = iota
	CANDIDATE = iota
	FOLLOWER  = iota
	STOP      = iota
)
