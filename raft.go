package raft

/*
Raft package implement the raft consensus algorithm. This algorithm is used for making concensus in
distributed system. The algorithm gaurantee that there would be only one leader a time.
This implementation runs three state one for rach leader , follower and candidate.
On the basis of the incoming signal an the timer events the consensus algorithm's state
is switched between these states
*/

import (
	"encoding/json"
	cluster "github.com/abhishekg16/cluster"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
	//"flag"
	"fmt"
	"encoding/gob"
	"math/rand"
	//	"bufio"
	"strconv"
	//"reflect"
	"math"
	kvs "github.com/abhishekg16/raft/kvstore"
)

// DEBUG variable will be setup to start server in
//debug which provide the differt kind of facilities
var LOG int
var timeOutCount int

// This method will remove all the entries which are greater then the
// lastIndex (inclusive)

func (c *consensus) purgeLog(s int64) {
	for i := s; i <= c.lastLogIndex; i++ {
		if LOG >= FINE {
			c.logger.Printf("Purge Log : Deleting key %v", i)
		}
		err := c.dbInterface.Delete(i)
		if err != nil && LOG >= INFO {
			c.logger.Printf("Purge Log : Error : Count not delete key %v", i)
		}
	}
	c.updateLastLogIndex()
}

func (c *consensus)validateCommand(command interface{}) (bool,error) {
	cmd , ok := command.(*Command)
	
	if ok == false {
		err := fmt.Errorf("Command not in proper format")
		return 	false,err
	}
	// this condition kept to support the previous cases
	if cmd.CmdId == 0 {
		return true, nil
	}
	if cmd.CmdId < c.commitIndex {
		err := fmt.Errorf("Command already applied")
		return 	false,err
	}
	if cmd.CmdId <= c.lastLogIndex {
		err := fmt.Errorf("Command in progess..")
		return false, err 
	}
	if cmd.CmdId != c.lastLogIndex+1 {
		err := fmt.Errorf("Command is out of order")
		return false, err
	}
	return true, nil
}

func (c *consensus) WriteToLocalLog(cmd interface{}) {
	c.lastLogIndex++
	c.lastLogTerm = c.currentTerm
	if LOG >= FINE {
		c.logger.Printf("Inserting in local log Index: %v, Term %v: Command: %v", c.lastLogIndex, c.lastLogTerm, cmd)
	}
	lEntry := LogEntry{Term: c.currentTerm, Command: cmd}
	err := c.dbInterface.Put(c.lastLogIndex, lEntry)
	if err != nil {
		if LOG >= INFO {
			c.logger.Printf("Error : WriteToLocalLog: %v \n", err)
		}
		c.lastLogIndex--
	}
}

func (c *consensus) verifyLastLogTermIndex(candidateLastLogTerm int64, candidateLastLogIndex int64) bool {
	if (c.lastLogTerm > candidateLastLogTerm) || (c.lastLogTerm == candidateLastLogTerm && c.lastLogIndex > candidateLastLogIndex) {
		return false
	} else {
		return true
	}
}

// check i
// return - Success - true if match successful
//			(only If success failed)
//			lastIndex
func (c *consensus) verifyPrevLogTermIndex(otherPrevLogTerm int64, otherPrevLogIndex int64) (bool, int64, error) {
	if otherPrevLogIndex == int64(0) {
		return true, 1, nil
	} else if val, err := c.dbInterface.Get(otherPrevLogIndex); err == nil {
		if val == nil {
			return false, c.lastLogIndex + 1, nil
		} else if otherPrevLogTerm == val.Term {
			return true, c.lastLogIndex + 1, nil
		} else {
			return false, otherPrevLogIndex, nil
		}
	} else {
		if LOG >= INFO {
			c.logger.Printf("Error : verifyPrevLogTermIndex \n")
		}
		return false, -1, err
	}
	return false, -1, nil
}

// This Token would be used for AppendEntries/HeartBeat Token
// Even though the Message Code would be able to identify the Actual Purpose of the Token

type AppendEntriesToken struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int64 // index for the logentry immediately precedding new one.
	PrevLogTerm  int64 // term of the prevLogEntry
	Entries      []LogEntry
	LeaderCommit int64 // tell follower till what index log has been commited and safe to execute on
	// state machine
}

// Response Token would be used for response of the AppendEntry/HeartBeat Token
type ResponseToken struct {
	Term      int64
	Success   bool
	NextIndex int64 // index which follower needs
	//nextTerm int64	// TODO :should be removed if not used
}

// VoteRequestToekn would be used to request the vote
type VoteRequestToken struct {
	Term         int64
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int64
}

// Vote Response Token would be used to respond the vote
type VoteResponseToken struct {
	Term        int64
	VoteGranted bool
}

type leaderStatus struct {
	pidOfLeader int
	tastHeard   time.Duration
	status      bool // true:live , false:dead
	votedFor    int  // -1 indicate not voted
}

// this stuct represent the data stored on the disk in persistent state
// This would be changed when logging would be added
type persistentData struct {
	Term     int64
	VotedFor int
	CommitIndex int64
	LastApplied int64
}

// This method reads the last persistent data data on the disk
func (c consensus) readDisk() (bool, error, persistentData) {
	msg, err := ioutil.ReadFile(c.filePath)
	if err != nil {
		if LOG >= INFO {
			c.logger.Printf("Raft %v :Error : %v", c.pid, err)
		}
		return false, err, persistentData{}
	}
	var data persistentData
	err = json.Unmarshal(msg, &data)
	if err != nil {
		if LOG >= INFO {
			c.logger.Printf("Raft %v :Error : %v", c.pid, err)
		}
		return false, err, persistentData{}
	}
	return true, nil, data
}

// This method writes the term on the disk
func (c consensus) writeTerm(currentTerm int64) (bool, error) {
	ok, err, data := c.readDisk()
	if ok == false {
		return ok, err
	}
	c.writeAll(currentTerm, data.VotedFor,data.CommitIndex,data.LastApplied)
	return true, nil
}

// this method write the voteFor variable on the disk
func (c consensus) writeVotedFor(votedFor int) (bool, error) {
	ok, err, data := c.readDisk()
	if ok == false {
		return ok, err
	}
	c.writeAll(data.Term, votedFor,data.CommitIndex,data.LastApplied)
	return true, nil
}

func (c *consensus) writeCommitIndex(commitIndex int64) (bool, error) {
	ok , err , data := c.readDisk() 
	if ok == false {
		return ok , err
	} 
	c.writeAll(data.Term, data.VotedFor, commitIndex,data.LastApplied)
	return true,nil
} 

func (c * consensus) writeLastApplied(lastApplied int64) (bool, error) {
	ok , err , data := c.readDisk() 
	if ok == false {
		return ok , err
	} 
	c.writeAll(data.Term, data.VotedFor, data.CommitIndex, lastApplied)
	return true,nil
}

func (c *consensus) writeAll(currentTerm int64, votedFor int, commitIndex int64, lastApplied int64) (bool, error) {
	data := persistentData{Term: currentTerm, VotedFor: votedFor,CommitIndex: commitIndex, LastApplied : lastApplied}
	msg, err := json.Marshal(data)
	if err != nil {
		if LOG >= INFO {
			c.logger.Println("Raft %v: Error : %v", c.pid, err)
		}
		return false, err
	}
	file, err := os.OpenFile(c.filePath, os.O_RDWR, 755)
	if err != nil {
		if LOG >= INFO {
			c.logger.Println("Raft %v: Error : %v", c.pid, err)
		}
		return false, err
	}
	defer file.Close()
	file.Truncate(0)
	n, err := file.Write(msg)
	// TODO : Check the case when the complete file can not be written
	// CHECK : whthe rthe write operation is atomic or not
	if n < len(msg) || err != nil {
		if LOG >= INFO {
			c.logger.Println("Raft %v: Error : %v", c.pid, err)
		}
		return false, err
	}

	return true, nil
}

// Consensun struct have all the fields related to raft instance
type consensus struct {
	pid int

	currentTerm int64

	state int // Leader, Follower or candidate

	votes int

	eTimeout time.Duration

	heartBeatInterval time.Duration

	lStatus leaderStatus

	server cluster.Server

	filePath string // RecordFile which store the persistent data

	dataDir string

	logDir string // path of the log files

	logger *log.Logger // logger this raft instance

	mutexOutbox sync.Mutex // mutex fox server out channel
	mutexInbox  sync.Mutex // mutex lock for server in channel
	msgId       int64

	outRaft chan interface{} // out channel used by client in order to put the message which need to
	// to be replicated, if the present instance in not leader rhe message would be
	// would be droped
	inRaft chan *LogItem //

	commitIndex int64 // index of highest log entry know to be commited

	lastApplied int64 // index of the highest log entry known to be commited

	// only for leader, and mast be re-initialized after each election
	nextIndex map[int]int64 // index of the next log entry to send to server for replication,
	// initialized to leader lastlog index +1
	matchIndex map[int]int64 // for each server index of the highest log entry know to be replicated on
	// that server

	lastLogIndex int64 //this keep track of the index of the highest log present in the disklog
	lastLogTerm  int64 //this keeps track of the last log term

	shutdownChan chan bool

	delayChan chan time.Duration

	prevLogIndex int64

	prevLogTerm int64

	dbPath string

	dbInterface *DBInterface
	
	kvStore *kvs.KVInterface
}

// Term return the current term
func (c *consensus) Term() int64 {
	return c.currentTerm
}

// IsLeader return true if the current raft instance is leader
func (c *consensus) IsLeader() bool {
	if c.lStatus.status == true && c.lStatus.pidOfLeader == c.pid {
		return true
	}
	return false
}

// Pid returns the Pid of the current raft instance
func (c *consensus) Pid() int {
	return c.pid
}

func (c *consensus) Delay(delay time.Duration) {
	c.delayChan <- delay
}

// Shutdown stops all the threds
func (c *consensus) Shutdown() {
	if LOG >= FINE {
		c.logger.Printf("Raft %v, Shutdown is called", c.pid)
	}
	c.shutdownChan <- true
	<-c.shutdownChan
	if LOG >= HIGH {
		c.logger.Printf("Raft %v, Shutdown", c.pid)
	}
}

func (c *consensus) Outbox() chan<- interface{} {
	return c.outRaft
}

func (c *consensus) Inbox() <-chan *LogItem {
	return c.inRaft
}

func (c *consensus) PrintLog() {
	c.dbInterface.PrintLog()
}

func (c *consensus) getMsgId() int64 {
	c.msgId++
	if c.msgId > 10000 {
		c.msgId = 0
	}
	return int64(c.pid*100*1000) + c.msgId
}

// Print the Configuration of the raft instance
func (c *consensus) GetConfiguration() {
	if LOG >= INFO {
		c.logger.Println("============= CONFIGURATION =================")
		c.logger.Printf("\t PID : %v \n", c.pid)
		c.logger.Printf("\t ELECTION TIMEOUT : %v \n", c.eTimeout)
		c.logger.Printf("\t DATA DIR : %v \n", c.dataDir)
		c.logger.Printf("\t HEART BEAT INTERVAL %v \n", c.heartBeatInterval)
		c.logger.Printf("\t Commit Index %v \n", c.commitIndex)
		c.logger.Printf("\t Last Applied  %v \n", c.lastApplied)
		c.logger.Printf("\t Last LogIndex  %v \n", c.lastLogIndex)
		c.logger.Printf("\t Log Database path  %v \n", c.dbPath)
		c.logger.Printf("\t Log File Path %v \n", c.filePath)
		c.logger.Printf("\t Peers %v \n", c.server.Peers())
	}
}

func (c *consensus) LastLogIndexAndTerm() (int64, int64) {
	return c.dbInterface.GetLastLogIndex(), c.dbInterface.GetLastLogTerm()
}

// this method check the server inbox channel and return the input Envelope to the coller
func (c *consensus) inbox() *cluster.Envelope {
	c.mutexInbox.Lock()
	defer c.mutexInbox.Unlock()
	var env *cluster.Envelope
	env = nil
	select {
	case env = <-c.server.Inbox():
		break
	case <-time.After(1 * time.Second):
		break
	}
	return env
}

// parse function takes it's own id and the path to the directory containg all the configuration files
func (c *consensus) parse(ownId int, path string) (bool, error) {
	if LOG >= FINE {
		log.Printf("Raft %v: Parsing The Configuration File in raft\n", c.pid)
	}
	path = path + "/raft.json"
	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		if LOG >= INFO {
			log.Printf("Raft %v: Parsing Failed %v\n", c.pid, err)
		}
		return false, err
	}
	dec := json.NewDecoder(file)
	for {
		var v map[string]interface{}
		if err := dec.Decode(&v); err == io.EOF || len(v) == 0 {
			if LOG >= FINE {
				log.Printf("Raft %v,Parsing Done !!!\n", c.pid)
			}
			return true, nil
		} else if err != nil {
			return false, err
		}
		var ok bool
		prop := v["property"]
		/*
			if prop == "myid" {
				t_pid := v["Value"].(float64)
				c.pid = int(t_pid)
			} else */
		if prop == "dataDir" {
			c.dataDir, ok = v["Value"].(string)
			if ok == false {
				if LOG >= INFO {
					log.Printf("Raft %v: Error : Can not retrieve dataDir \n", c.pid)
				}
			}
		} else if prop == "Etimeout" {
			t_timeout, ok := v["Value"].(float64)
			if ok == false {
				if LOG >= INFO {
					log.Printf("Raft %v: Error : Can not retrieve Eelection Timeout \n", c.pid)
				}
			} else {
				c.eTimeout = time.Duration(t_timeout) * time.Millisecond
			}
		} else if prop == "hfre" {
			t_hfre, ok := v["Value"].(float64)
			if ok == false {
				if LOG >= INFO {
					log.Printf("Raft %v: Error : Can not retrieve href \n", c.pid)
				}
			} else {
				c.heartBeatInterval = time.Duration(t_hfre) * time.Millisecond
			}
		} else if prop == "logDir" {
			c.logDir, ok = v["Value"].(string)
			if ok == false {
				if LOG >= INFO {
					log.Printf("Raft %v: Error : Can not retrieve logDir \n", c.pid)
				}
			}
		}
	}
}

func init() {
	log.Println("Registering Interfaces in gob")
	gob.Register(VoteRequestToken{})
	gob.Register(VoteResponseToken{})
	gob.Register(AppendEntriesToken{})
	gob.Register(ResponseToken{})
	gob.Register(LogEntry{})
	gob.Register(LogItem{})
	gob.Register(Command{})
	gob.Register(Result{})
}

// leader tracker runs the timer in case timeout happen it will send and message on the out channel
// and will restun
func (c *consensus) leaderTracker(in chan bool, out chan bool) {
	if LOG >= HIGH {
		c.logger.Printf("Raft %v: Leader Tracker : Got Enable Signal\n", c.pid)
	}
	// Each server will use it's pid as seed to all will have different
	// sequesnce of random values
	// TODO : Timeut for first time must be at lower bound
	var randTimeout time.Duration 
	if timeOutCount > 0 {
		randTimeout = c.eTimeout + time.Duration(float64(c.eTimeout.Nanoseconds()*int64(math.Pow(2,float64(timeOutCount))))*(float64(rand.Intn(10000))/10000))*time.Nanosecond
	} else {
		randTimeout = c.eTimeout
	}
	if LOG >= HIGH {
		c.logger.Printf("Raft %v: Leader Tracker : Timeout duration %v\n", c.pid, randTimeout)
	}
LOOP:
	for {
		select {
		case <-time.After(randTimeout):
			if LOG >= HIGH {
				c.logger.Printf("Raft %v: Leader Tracker : Election Timeout\n", c.pid)
			}
			out <- true
			break LOOP
		case val := <-in:
			if val == false {
				if LOG >= HIGH {
					c.logger.Printf("Raft %v: Leader Tracker : Shutdown", c.pid)
				}
				return
			}
			if LOG >= HIGH {
				c.logger.Printf("Raft %v: Leader Tracker : GotHB", c.pid)
			}
			continue
		}
	}
}

func getMessage(env *cluster.Envelope) *cluster.Message {
	tMsg := env.Msg
	return &tMsg
}

func (c *consensus) enterInLocalLog(currentIndex int64, entries []LogEntry) (int64, error) {
	for i := 0; i < len(entries); i++ {
		le := entries[i]
		err := c.dbInterface.Put(currentIndex, le)
		if err != nil {
			if LOG >= INFO {
				c.logger.Println("Follower State : Count Not Insert in localLog ")
			}
			// TODO : Make this operation atomic
			return currentIndex - 1, err
		}
		if LOG >= FINE {
			c.logger.Println("Follower State : Inserting in localLog, Intex : %v, Entry : %v ", currentIndex, le)
		}
		currentIndex++
	}
	return currentIndex - 1, nil
}

// follower method implements the follower state
func (c *consensus) follower() int {
	if LOG >= HIGH {
		c.logger.Printf("Raft %v: Follower State : Follow is Activated\n", c.pid)
	}

	in_leaderTracker := make(chan bool, 1)
	out_leaderTracker := make(chan bool, 1)
	go c.leaderTracker(in_leaderTracker, out_leaderTracker)

	c.updateLastLogIndex()
	for {
		c.updateLastAppliedIndex()
		
		select {
		case env := <-c.server.Inbox():
			if LOG >= FINE {
				c.logger.Println("Follower State :  message arrived")
			}
			msg := getMessage(env)
			switch {
			case msg.MsgCode == APPEND_ENTRY:
				if LOG >= FINE {
					c.logger.Printf("Follower State : Append Received from %+v  \n", env)
				}

				data, ok := (msg.Msg).(AppendEntriesToken)
				if ok == false {
					if LOG >= INFO {
						c.logger.Printf("Follower State : Error: Append Entries MssCode with different MsgToken %v \n", env.Pid)
					}
					continue
				}
				term := int64(data.Term)
				if c.currentTerm < term {
					if LOG >= INFO {
						c.logger.Println("Follower State : Got AppendEntry From Higher Term")
					}
					in_leaderTracker <- true

					// updating Leader Status
					c.lStatus.status = true
					c.lStatus.pidOfLeader = data.LeaderId
					c.markVote(term, data.LeaderId)
					result, nextIndexRequest, err := c.verifyPrevLogTermIndex(data.PrevLogTerm, data.PrevLogIndex)
					if err != nil {
						if LOG >= INFO {
							c.logger.Printf("Follower State : Error: \n")
						}
						continue
					} else if nextIndexRequest == -1 {
						if LOG >= INFO {
							c.logger.Printf("Follower State : Error: \n")
						}
						continue
					}
					if result == false || data.Entries == nil || len(data.Entries) == 0 {
						c.sendNewResponseToken(env.Pid, result, nextIndexRequest)
						c.logger.Println("Follower State : Append Entry Without Log : Reply %v",result)
						continue
					}

					c.purgeLog(data.PrevLogIndex + 1)

					currentIndex := data.PrevLogIndex + 1
					c.lastLogIndex, err = c.enterInLocalLog(currentIndex, data.Entries)
					if err != nil {
						continue
					}

					// update the commit Index
					if data.LeaderCommit > c.commitIndex {
						c.commitIndex = data.LeaderCommit
						if c.commitIndex > c.lastLogIndex {
							c.commitIndex = c.lastLogIndex
						}
					}

					if LOG >= FINE {
						c.logger.Println("Follower State : Append Entry With Log : Reply Success ")
					}
					c.sendNewResponseToken(env.Pid, true, c.lastLogIndex+1)
				} else if c.currentTerm > term {
					if LOG >= FINE {
						c.logger.Println("Follower State : Append Entry from lower term  : Reply Fail ")
					}
					c.sendNewResponseToken(env.Pid, false, c.lastLogIndex+1)
				} else if c.currentTerm == term {
					// if in current term recived HB from some different Leader
					if c.lStatus.status == true && c.lStatus.pidOfLeader != data.LeaderId {
						if LOG >= INFO {
							c.logger.Println("Follower State : Error: More then one leader in same term")
						}
						os.Exit(0)
						continue
					}

					in_leaderTracker <- true
					if c.lStatus.status == false {
						c.lStatus.status = true
						c.lStatus.pidOfLeader = data.LeaderId
						c.markVote(term, data.LeaderId)
					}

					result, nextIndexRequest, err := c.verifyPrevLogTermIndex(data.PrevLogTerm, data.PrevLogIndex)
					if err != nil {
						if LOG >= INFO {
							c.logger.Printf("Follower State : Error: \n")
						}
						continue
					} else if nextIndexRequest == -1 {
						if LOG >= INFO {
							c.logger.Printf("Follower State : Error: \n")
						}
						continue
					}
					if result == false || data.Entries == nil || len(data.Entries) == 0 {
						c.logger.Println("Follower State : Append Entry Without Log from %v: Reply %v",result, env.Pid)
						c.sendNewResponseToken(env.Pid, result, nextIndexRequest)
						continue
					}

					c.purgeLog(data.PrevLogIndex + 1)

					currentIndex := data.PrevLogIndex + 1
					c.lastLogIndex, err = c.enterInLocalLog(currentIndex, data.Entries)
					if err != nil {
						continue
					}

					// update the commit Index
					if data.LeaderCommit > c.commitIndex {
						c.commitIndex = data.LeaderCommit
						if c.commitIndex > c.lastLogIndex {
							c.commitIndex = c.lastLogIndex
						}
					}

					if LOG >= FINE {
						c.logger.Println("Follower State : Append Entry With Log : Reply Success ")
					}
					c.sendNewResponseToken(env.Pid, true, c.lastLogIndex+1)
				}
			case msg.MsgCode == VOTEREQUEST:
				if LOG >= HIGH {
					c.logger.Println("Follower State: Got Vote Request from ", env.Pid)
				}
				data, ok := (msg.Msg).(VoteRequestToken)
				if ok == false {
					if LOG >= INFO {
						c.logger.Printf("Follower State : Append Entries MssCode with different MsgToken %v \n", env.Pid)
					}
					continue
				}
				term := int64(data.Term)
				// TODO : Handle the second vote request coming from the same sender second time
				if c.currentTerm > term {
					if LOG >= HIGH {
						c.logger.Println("Follower State : Request With Lower term..Rejecting VoteRequest")
					}
					c.sendNewVoteResponseToken(env.Pid, false)
					// if not voted or voted for candidate Id )
				} else if c.currentTerm < term {
					if LOG >= HIGH {
						c.logger.Println("Follower State : Request from higer term..Updatinf own term")
					} 
					if c.verifyLastLogTermIndex(data.LastLogTerm, data.LastLogIndex) {
						in_leaderTracker <- true
						c.markVote(term, int(data.CandidateId))
						c.lStatus.status = false
						if LOG >= HIGH {
							c.logger.Println("Follower State : LastLog Varification: Successful Giving Vote")
						}
						c.sendNewVoteResponseToken(env.Pid, true)
					} else {
						c.currentTerm = term
						c.markVote(c.currentTerm,-1)
						c.lStatus.status = false
						if LOG >= HIGH {
							c.logger.Println("Follower State : LastLog Varification: Failed : Rejecting request")
						}
						c.sendNewVoteResponseToken(env.Pid, false)
					}
				} else if (c.lStatus.votedFor == -1) || c.lStatus.votedFor == data.CandidateId {
					if LOG >= HIGH {
						if c.lStatus.votedFor == -1 {
							c.logger.Println("Follower State : Not Voted Yet")
						} else {
							c.logger.Println("Follower State : Already voted to same candidate")
						}
					}
					if c.verifyLastLogTermIndex(data.LastLogTerm, data.LastLogIndex) {
						in_leaderTracker <- true
						c.markVote(term, int(data.CandidateId))
						if LOG >= HIGH {
							c.logger.Println("Follower State : LastLog Term and Index Varification: Successful Giving Vote")
						}
						c.sendNewVoteResponseToken(env.Pid, true)
					} else {
						if LOG >= HIGH {
							c.logger.Println("Follower State : LastLog Term and Index Varification: Failed : Rejecting request")
						}
						c.sendNewVoteResponseToken(env.Pid, false)
					}
				} else {
					if LOG >= HIGH {
						c.logger.Println("Follower State : Alreadly voted to different candidate")
					}
					c.sendNewVoteResponseToken(env.Pid, false)
				}
			case msg.MsgCode == VOTERESPONSE:
				if LOG >= HIGH {
					c.logger.Printf("Follower State : Got vote response from... rejecting", env.Pid)
				}
			case msg.MsgCode == RESPONSE:
				if LOG >= HIGH {
					c.logger.Println("Follower State : Got Appned Entry response ... rejecting")
				}
			default:
				c.logger.Println("Case Not handled")
				os.Exit(0)
			}
		case <-out_leaderTracker:
			return CANDIDATE
		case <-c.outRaft:
			if LOG >= HIGH {
				c.logger.Printf("Follower State : Message came at outRaft\n")
			}
			lItem := LogItem{Index: -1, Data: c.lStatus.pidOfLeader}
			c.inRaft <- &lItem
		case <-c.shutdownChan:
			in_leaderTracker <- false
			if LOG >= HIGH {
				c.logger.Printf("Follower State : Stopped\n")
			}
			return STOP
		}
	}
}

func (c *consensus) markVote(term int64, candidateId int) {
	c.currentTerm = term
	c.lStatus.votedFor = candidateId
	c.writeAll(c.currentTerm, c.lStatus.votedFor,c.commitIndex,c.lastApplied)
}

func (c *consensus) sendVoteRequestToken(pid int) {
	voteReq := VoteRequestToken{Term: c.currentTerm, CandidateId: c.pid, LastLogIndex: c.lastLogIndex, LastLogTerm: c.lastLogTerm}
	reqMsg := cluster.Message{MsgCode: VOTEREQUEST, Msg: voteReq}
	msgId := c.getMsgId()
	replyEnv := &cluster.Envelope{Pid: pid, MsgId: msgId, MsgType: cluster.CTRL, Msg: reqMsg}
	c.sendMessage(replyEnv)
}

// follower method implements the candidate state
func (c *consensus) candidate() int {
	if LOG >= HIGH {
		c.logger.Println("Candidate State : System in candidate state")
	}
	followerPid := c.server.Peers()
	voteResponseMap := make(map[int]bool, len(followerPid))
	voteCount := 1
	c.markVote(c.currentTerm+1, c.pid)
	c.lStatus.status = false
	majority := (len(followerPid) / 2) + 1
	if LOG >= HIGH {
		c.logger.Printf("Candidate State : Required Majority : %v , Term :%v\n", majority, c.currentTerm)
	}

	in_leaderTracker := make(chan bool, 1)
	out_leaderTracker := make(chan bool, 1)
	go c.leaderTracker(in_leaderTracker, out_leaderTracker)

	// update LastLogIndex
	c.lastLogIndex = c.dbInterface.GetLastLogIndex()
	c.lastLogTerm = c.dbInterface.GetLastLogTerm()

	c.sendVoteRequestToken(cluster.BROADCAST)

	for {
		select {
		case env := <-c.server.Inbox():
			msg := getMessage(env)
			switch {
			// Append entries means leader has been selected
			case msg.MsgCode == APPEND_ENTRY:
				data, ok := msg.Msg.(AppendEntriesToken)
				if ok == false {
					if LOG >= INFO {
						c.logger.Println("Candidate State : Message Code and Msgtype Mismatch")
					}
					continue
				}

				// canmdidate will treat all the AppendEntries as HB only
				if c.currentTerm <= int64(data.Term) {
					if LOG >= HIGH {
						c.logger.Println("Candidate State : Recived HeatBeat with higher term or equal term==> Leader Elected.")
					}
					in_leaderTracker <- false
					c.lStatus.pidOfLeader = int(data.LeaderId)
					c.lStatus.status = true
					c.currentTerm = int64(data.Term)
					c.markVote(int64(data.Term), int(data.LeaderId))
					if LOG >= FINE {
						c.logger.Println("Candidate State : Enabling Follower State")
					}
					c.sendNewResponseToken(env.Pid, true, c.lastLogIndex+1)
					return FOLLOWER
				} else {
					c.logger.Println("Candidate State :Recived HeatBeat with lower Term and rejecting .")
					c.sendNewResponseToken(env.Pid, false, c.lastLogIndex+1)
				}
			case msg.MsgCode == VOTEREQUEST:
				if LOG >= HIGH {
					c.logger.Println("Candidate State : Vote Request Recieved")
				}
				data, ok := (msg.Msg).(VoteRequestToken)
				if ok == false {
					if LOG >= INFO {
						c.logger.Println("Candidate State : Message Code and Msgtype Mismatch")
					}
					continue
				}
				term := int64(data.Term)

				if c.currentTerm >= term {
					if LOG >= HIGH {
						c.logger.Println("Candidate State : Vote Request from lower term or equal term ... Rejecting")
					}
					c.sendNewVoteResponseToken(env.Pid, false)
				} else if c.currentTerm < term {
					if LOG >= HIGH {
						c.logger.Println("Candidate State : Vote Request with  higher")
					}
					if c.verifyLastLogTermIndex(data.LastLogTerm, data.LastLogIndex) {
						if LOG >= HIGH {
							c.logger.Println("Candidate State : The vericication of Last Term and Index: Succeded : Giving Vote")
						}
						c.markVote(term, int(data.CandidateId))
						c.sendNewVoteResponseToken(env.Pid, true)
						in_leaderTracker <- false
						if LOG >= HIGH {
							c.logger.Println("Candidate State : Enabling Follower State")
						}
						return FOLLOWER
					} else {
						if LOG >= HIGH {
							c.logger.Println("Candidate State : The vericication of Last Term and Index failed")
						}
						c.sendNewVoteResponseToken(env.Pid, false)
					}
				}
			case msg.MsgCode == VOTERESPONSE:
				data, ok := (msg.Msg).(VoteResponseToken)
				if LOG >= INFO && ok == false {
					c.logger.Println("Candidate State :  MsgCode and Massge token mismatch")
				}
				if LOG >= HIGH {
					c.logger.Printf("Candidate State : Got a vote Response from %v\n", env.Pid)
				}
				c.logger.Printf("%+v\n", data)
				term := int64(data.Term)
				voteGranted := data.VoteGranted
				if term < c.currentTerm {
					if LOG >= HIGH {
						c.logger.Println("Candidate State : Got a vote Response with lower term... Rejecting")
					}
				} else if term > c.currentTerm {
					if LOG >= HIGH {
						c.logger.Println("Candidate State : Vote Response from Higher Term.. must be negative")
					}
					c.markVote(term, -1)
					if LOG >= HIGH {
						c.logger.Println("Candidate State : Downgrading to follower")
					}
					in_leaderTracker <- false
					return FOLLOWER
				} else if voteGranted == true && c.currentTerm == term {
					_, ok := voteResponseMap[env.Pid]
					if ok == false {
						voteResponseMap[env.Pid] = true
						voteCount++
						c.logger.Printf("Candidate State : Vote Count %v \n ", voteCount)
						if LOG >= HIGH {
							c.logger.Printf("Candidate State : Vote Count %v \n ", voteCount)
						}
					}
					if voteCount >= majority {
						if LOG >= HIGH {
							c.logger.Println(c.pid, " Candidate State : has been elected as a leader")
						}
						c.lStatus.pidOfLeader = c.pid
						c.lStatus.status = true
						in_leaderTracker <- false
						return LEADER
					}
				}
			default:
				c.logger.Println("Case Not handled")
				os.Exit(0)
			}
		case <-out_leaderTracker:
			if LOG >= HIGH {
				c.logger.Println("Candidate State : Election Timeout restart Election")
			}
			return CANDIDATE
		case <-c.shutdownChan:
			in_leaderTracker <- false
			if LOG >= HIGH {
				c.logger.Printf("Candiate State : Stopped\n")
			}
			return STOP
		}
	}
}

func (c *consensus) sendAppendEntry(heartBeat bool) {
	for pid, nIndex := range c.nextIndex {
		if c.lastLogIndex >= nIndex {
			// send the Append Entry
			lEntry, err := c.dbInterface.Get(nIndex)
			if err != nil {
				if LOG > INFO {
					c.logger.Printf("Error : Cound not send log entry")
				}
				return
			}
			if lEntry == nil {
				if LOG > INFO {
					c.logger.Printf("Error : No entry present at the log for nIndex : %v", nIndex)
				}
				return
			}

			// logIndex can not be less then 1
			prevLogIndex := nIndex - 1
			var prevLogTerm int64
			if prevLogIndex == 0 {
				prevLogTerm = int64(0)
			} else {
				prevLogTerm, err = c.dbInterface.GetTerm(prevLogIndex)
				if LOG >= FINE {
					c.logger.Printf("PrevLogTerm should be %v\n",prevLogTerm)
				}
				if err != nil || prevLogTerm == -1 {
					if LOG >= INFO {
						c.logger.Printf("Error : Cound not send log entry")
					}
					return
				}

			}
			entries := make([]LogEntry, 0)
			entries = append(entries, *lEntry)
			aEntry := AppendEntriesToken{Term: c.currentTerm, LeaderId: c.pid, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: c.commitIndex}
			msg := cluster.Message{MsgCode: APPEND_ENTRY, Msg: aEntry}
			env := &cluster.Envelope{Pid: pid, MsgType: cluster.CTRL, Msg: msg}
			if LOG >= INFO {
				c.logger.Printf("Sending HB %+v \n", env)
			}
			c.sendMessage(env)
		} else if heartBeat == true {
			//simplySendHB with out entries
			prevLogIndex := nIndex - 1
			var prevLogTerm int64
			var err error
			if prevLogIndex == int64(0) {
				prevLogTerm = int64(0)
			} else {
				prevLogTerm, err = c.dbInterface.GetTerm(prevLogIndex)
				if err != nil || prevLogTerm == -1 {
					if LOG >= INFO {
						c.logger.Printf("Error : Cound not send log entry")
					}
					return
				}
			}
			aEntry := AppendEntriesToken{Term: c.currentTerm, LeaderId: c.pid, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, LeaderCommit: c.commitIndex}
			msg := cluster.Message{MsgCode: APPEND_ENTRY, Msg: aEntry}
			env := &cluster.Envelope{Pid: pid, MsgType: cluster.CTRL, Msg: msg}
			if LOG >= INFO {
				c.logger.Printf("Sending HB %+v \n", env)
			}
			c.sendMessage(env)
		}
	}
}

func (c *consensus) updateCommitIndex() {
	nextCommitIndex := c.commitIndex + 1
	// search for largest Index which can be commited
	majority := len(c.nextIndex) / 2

	if LOG >= HIGH {
		c.logger.Printf("Updating CommitIndex\n")
		c.logger.Printf("NextIndex : %v\n", c.nextIndex)
		c.logger.Printf("MatchIndex : %v\n", c.matchIndex)
	}

	for {
		count := 0
		for _, v := range c.matchIndex {
			if v >= nextCommitIndex {
				count++
			}
		}
		if count >= majority {
			nextCommitIndex++
		} else {
			nextCommitIndex--
			break
		}
	}

	if nextCommitIndex > c.commitIndex {
		if LOG >= FINE {
			c.logger.Printf("nextCommitIndex: %v, currentCommitIndex : %v\n", nextCommitIndex, c.commitIndex)

		}
		term, err := c.dbInterface.GetTerm(nextCommitIndex)
		if err != nil {
			if LOG >= INFO {
				c.logger.Printf("Error : %v\n", err)
			}
			return
		} else if term == -1 {
			c.logger.Printf(" Error : Invalid Term\n")
		} else if term == c.currentTerm {
			c.commitIndex = nextCommitIndex
			c.writeCommitIndex(c.commitIndex)
		}
	}
}

// This method execute the command on the KVstore and the 
// then return the result
func (c *consensus) executeCommand(cmd interface{} ) (*Result , error){
	inCmd, ok := cmd.(Command)
	if ok == false {
		if LOG >= INFO {
			c.logger.Printf("Raft %v : Command format not supported\n")
		}
		err := fmt.Errorf("Command Format not supported")
		return nil, err
	} 
	
	var newCmd kvs.Command
	
	newCmd.Cmd = inCmd.Cmd
	newCmd.Key = inCmd.Key
	newCmd.Value = inCmd.Value
	
	value, err := c.kvStore.ExecuteCommand(&newCmd)
	
	result :=  Result {Error : err, Value : value}
	
	return &result, err
}

func (c *consensus) updateLastAppliedIndex() {
	if LOG >= HIGH {
			c.logger.Printf("UpdatingLastApplied : Apply Log in State Machie,LastLogIndex : %v, CommitIndex : %v , LastAppliedIndex %v", c.lastLogIndex, c.commitIndex, c.lastApplied)
	}
	if c.commitIndex > c.lastApplied {
		for i := c.lastApplied + 1; i <= c.commitIndex; i++ {
			if LOG >= HIGH {
				c.logger.Printf("UpdatingLastApplied: Apply Log in State Machie, LogIndex %v", i)
			}
			lEntry, err := c.dbInterface.Get(i)
			if err != nil  || lEntry == nil{
				if lEntry == nil {
					c.PrintLog()
				}
				c.logger.Printf("Raft %v: Cound not fetach command to apply")
				c.lastApplied = i-1
				_ , err = c.writeLastApplied(c.lastApplied)
				if err != nil {
					c.logger.Printf("Raft %v: Error in writing last applied index")
				}
				return
			}
			
			cmd := lEntry.Command 
			result , err := c.executeCommand(cmd)
			if err != nil {
				c.logger.Printf("Raft %v: Command Execution failed")
				c.lastApplied = i-1
				_ , err = c.writeLastApplied(c.lastApplied)
				if err != nil {
					c.logger.Printf("Raft %v: Error in writing last applied index")
				}
				return
			}
			cmdReply := LogItem{Index: i, Data: *result} 
			c.inRaft <- &cmdReply
			if LOG >= HIGH {
				c.logger.Printf("UpdatingLastApplied: Replied to client %+v", cmdReply)
			}
		}
		c.lastApplied = c.commitIndex
		_, err := c.writeLastApplied(c.lastApplied)
		if err != nil {
					c.logger.Printf("Raft %v: Error in writing last applied index")
		}
	}
}

func (c *consensus) updateLastLogIndex() {
	c.lastLogIndex = c.dbInterface.GetLastLogIndex()
	c.lastLogTerm = c.dbInterface.GetLastLogTerm()
}

func (c *consensus) initializeNextAndMatchIndex() {
	peers := c.server.Peers()
	for i := 0; i < len(peers); i++ {
		c.nextIndex[peers[i]] = c.lastLogIndex + 1
	}
	// initailizing match index
	for i := 0; i < len(peers); i++ {
		c.matchIndex[peers[i]] = 0
	}
}

// following method implements the leader state
func (c *consensus) leader() int {
	if LOG >= HIGH {
		c.logger.Println("Leader State :System in Leader State")
	}

	c.lStatus.status = true
	c.lStatus.pidOfLeader = c.pid

	// update lastLog term and index
	c.updateLastLogIndex()

	//initialize the nextIndex for each server
	c.initializeNextAndMatchIndex()

	if LOG >= FINE {
		c.logger.Printf("Leader State : nextIndex %v \n", c.nextIndex)
		c.logger.Printf("Leader State : matchIndex %v \n", c.matchIndex)
	}

	// sendIntialHB
	c.sendAppendEntry(true)
	lastHeartBeatTimeStamp := time.Now()

	for {
		//updateCommit Index and lastApplied Index
		c.updateCommitIndex()
		c.updateLastAppliedIndex()

		// sendHB if necessary
		t := lastHeartBeatTimeStamp
		prevTimeStamp := t.Add(c.heartBeatInterval)
		if LOG >= FINE {
			//c.logger.Printf("Leader State : last HB TimeS : %v , prevTimeS : %v , NOW : %v \n", t, prevTimeStamp, time.Now())
		}
		if prevTimeStamp.Before(time.Now()) {
			// need to send a HB
			if LOG >= FINE {
				c.logger.Printf("Leader State : Sending HB\n")
			}
			lastHeartBeatTimeStamp = time.Now()
			c.sendAppendEntry(true)
		}
		select {
		case env := <-c.server.Inbox():
			msg := getMessage(env)
			switch {
			case msg.MsgCode == APPEND_ENTRY:
				data, ok := (msg.Msg).(AppendEntriesToken)
				if LOG >= INFO && ok == false {
					c.logger.Println("Leader State : Mismatach in MsgCode and MsgToken")
				}
				if LOG >= HIGH {
					c.logger.Printf("Leader State : Received heart beat from %v", data.LeaderId)
				}
				term := int64(data.Term)
				if c.currentTerm < term {
					if LOG >= INFO {
						c.logger.Println("Leader State : Receive heart beat with higher term")
					}
					// another leader is present in with higher term
					c.markVote(term, int(data.LeaderId))
					c.lStatus.status = true
					c.lStatus.pidOfLeader = data.LeaderId
					c.logger.Println("Leader State : Downgrading to follower")
					c.sendNewResponseToken(env.Pid, false, c.lastLogIndex)
					return FOLLOWER
				} else if c.currentTerm == term {
					if LOG >= INFO {
						c.logger.Println("Leader State : ERROR : Receive heart beat with equal term")
						c.logger.Println("Leader State : ERROR : Two Leader in same term")
					}
					os.Exit(1)
					// check log length
				} else {
					c.logger.Println("Leader State : Receive heart beat with lower term")
					c.sendNewResponseToken(env.Pid, false, c.lastLogIndex)
				}
			case msg.MsgCode == RESPONSE:
				data, ok := (msg.Msg).(ResponseToken)
				if ok == false {
					if LOG >= INFO {
						c.logger.Println("Leader State : ERROR : MsgCode and Message token mismatech")
					}
				}
				term := int64(data.Term)
				//c.logger.Println("Leader State : Response Message Id %v, Msg %v  ", env.MsgId ,data,)
				if c.currentTerm < term {
					c.logger.Println("Leader State : Got Negative HB Response")
					c.logger.Println("Leader State : Downgrading to follower")
					c.markVote(term, -1)
					c.lStatus.status = false
					return FOLLOWER
				} else if c.currentTerm > term {
					c.logger.Println("Leader State : Got Response from lower term... rejecting")
				} else if data.Success == true {
					// LogResponse has been applied
					if LOG >= HIGH {
						c.logger.Printf("Leader State : Got Replication Response from %v", env.Pid)
					}
					// HB Respose success : LogResponse : Applied
					replier := env.Pid
					c.nextIndex[replier] = data.NextIndex
					if ok == false {
						if LOG >= INFO {
							c.logger.Printf("Leader State : Cound not find the log Entry for the %v", replier)
						}
						continue
					}
					c.matchIndex[replier] = data.NextIndex - 1
					if LOG >= HIGH {
						c.logger.Printf("Leader State : Raft : %v Accepted Log : nextIndex : %v", env.Pid, data.NextIndex)
					}
				} else if data.Success == false {
					// HB Respose : Failed : LogRepose : decresse it
					replier := env.Pid
					c.nextIndex[replier] = data.NextIndex
					if LOG >= HIGH {
						c.logger.Printf("Leader State : Raft : %v replied the inconsistent Log for HB : nextIndex : %v", env.Pid, data.NextIndex)
					}
				}
			case msg.MsgCode == VOTEREQUEST:
				data := (msg.Msg).(VoteRequestToken)
				term := int64(data.Term)
				if LOG >= HIGH {
					c.logger.Printf("Leader State :Leader received voteRequest from %v\n", data.CandidateId)
				}
				if c.currentTerm > term {
					if LOG >= HIGH {
						c.logger.Println("Leader State : The received request have less term")
						c.logger.Println("Leader State : Sending Negative Reply")
					}
					c.sendNewVoteResponseToken(env.Pid, false)
				} else if c.currentTerm == term {
					if LOG >= HIGH {
						c.logger.Println("Leader State : Vote Request with equal term")
						c.logger.Println("Leader State : Sending Negative Reply")
					}
					c.sendNewVoteResponseToken(env.Pid, false)
				} else {
					// request from higher term
					if LOG >= INFO {
						c.logger.Println("Leader State : Vote Request from higher term")
					}
					if c.verifyLastLogTermIndex(data.LastLogTerm, data.LastLogTerm) {
						if LOG >= INFO {
							c.logger.Println("Leader State : accepting vote ")
							c.logger.Println("Leader State : Downgrading to follower ")
						}
						c.lStatus.status = false
						c.markVote(term, data.CandidateId)
						c.sendNewVoteResponseToken(env.Pid, true)
						return FOLLOWER
					} else {
						if LOG >= INFO {
							c.logger.Println("Leader State : Leader More uptodate...sending Negative Reply")
							c.logger.Println("Leader State : Because of higher Temr can not be leader any more moving to Follower")
						}
						c.sendNewVoteResponseToken(env.Pid, false)
						c.lStatus.status = false
						c.markVote(term, -1)
						return FOLLOWER
					}
				}
			case msg.MsgCode == VOTERESPONSE:
				if LOG >= HIGH {
					c.logger.Println("Leader State : Got vote response from %v.. rejecting", env.Pid)
				}
			default:
				if LOG >= INFO {
					c.logger.Println("Leader State : Case Not handled")
				}
				os.Exit(0)
			}
		case delay := <-c.delayChan:
			//delay_heartBeatManager <- delay
			if LOG >= HIGH {
				c.logger.Printf(" Leader State : Introducing Delay %v", delay)
			}
			time.Sleep(delay)
		case <-c.shutdownChan:
			//in_heartBeatManager <- true
			if LOG >= HIGH {
				c.logger.Printf("Leader State : Stopped\n")
			}
			return STOP
		case <-time.After(c.heartBeatInterval):
			continue
		case cmd := <-c.outRaft:
			if LOG >= HIGH {
				c.logger.Printf("Leader State : Got commod for replication %+v\n",cmd)
			}
			ok , err := c.validateCommand(cmd) 
			if ok == false {
				if LOG >= INFO {
					c.logger.Printf("Command Validation failed\n")
				}
				cmdReply := LogItem{Index: -2, Data: err} 
				c.inRaft <- &cmdReply
				continue
			} 
			if LOG >= INFO {
					c.logger.Printf("Command Validation sucessful\n")
			}
			c.WriteToLocalLog(cmd)
			// as new request has been arrieved send a new entry
			c.sendAppendEntry(false)
		}
	}
}

// sentMessage send the message to underlying cluster
func (c *consensus) sendMessage(env *cluster.Envelope) bool {
	c.mutexOutbox.Lock()
	c.server.Outbox() <- env
	c.mutexOutbox.Unlock()
	return true
}

// getNewVoteResponseToken return a new VoteRequestToken
func (c *consensus) getNewVoteResponseToken(flag bool) *cluster.Message {
	vote := VoteResponseToken{c.currentTerm, flag}
	replyMsg := cluster.Message{MsgCode: VOTERESPONSE, Msg: vote}
	return &replyMsg
}

func (c *consensus) sendNewVoteResponseToken(pid int, flag bool) {
	replyMsg := c.getNewVoteResponseToken(flag)
	//msgId = c.getMsgId()
	replyEnv := &cluster.Envelope{Pid: pid, MsgType: cluster.CTRL, Msg: *replyMsg}
	c.sendMessage(replyEnv)
}

func (c *consensus) getNewResponseToken(flag bool, nextIndex int64) *cluster.Message {
	vote := ResponseToken{Term: c.currentTerm, Success: flag, NextIndex: nextIndex}
	replyMsg := cluster.Message{MsgCode: RESPONSE, Msg: vote}
	return &replyMsg
}

func (c *consensus) sendNewResponseToken(pid int, flag bool, nextIndex int64) {
	replyMsg := c.getNewResponseToken(flag, nextIndex)
	//msgId = c.getMsgId()
	replyEnv := &cluster.Envelope{Pid: pid, MsgType: cluster.CTRL, Msg: *replyMsg}
	c.sendMessage(replyEnv)
}

// initialize the raft instance
func (c *consensus) initialize(pid int, path string, server *cluster.Server, kvStorePath string,isRestart bool) (bool, error) {

	ok, err := c.parse(pid, path)
	if ok == false {
		if LOG >= INFO {
			c.logger.Printf("Raft %v: Error : Parsing Failed, %v\n", c.pid, err)
		}
		return false, err
	}

	logFileName := c.logDir + "/" + LOGFILENAME + strconv.Itoa(c.pid)
	//log.Println(logFileName)
	// TODO add Method to check the existance of log file
	file, err := os.Create(logFileName)
	if err != nil {
		if LOG >= INFO {
			log.Printf("Raft %v: Error : %v\n", c.pid, err)
		}
	}
	//c.logger = log.New(file, "Log: ", log.Ldate|log.Ltime|log.Lshortfile)
	c.logger = log.New(file, "Log: ", log.Ltime|log.Lshortfile)

	if LOG >= HIGH {
		c.logger.Printf("Raft %v: Intializing the Raft , Restart : %v\n", pid, isRestart)
	}

	c.server = *server

	//TODO :Chech this condition
	// Each of the server will start as the follower, or it sould be previously failed condition
	c.state = FOLLOWER
	c.filePath = c.dataDir + "/" + FILENAME + strconv.Itoa(c.pid)

	c.dbPath = c.dataDir + "/" + DATABASE_NAME + strconv.Itoa(c.pid)

	// Check if file exist from previos failure
	if _, err := os.Stat(c.filePath); (!isRestart) && os.IsExist(err) {
		if LOG >= HIGH {
			c.logger.Printf("Raft %v :Restarting\n", c.pid)
		}
		if LOG >= HIGH {
			c.logger.Printf("Raft %v :Meta data file already exist\n", c.pid)
		}
		ok, err, data := c.readDisk()
		if ok == true {
			c.currentTerm = data.Term
			c.lStatus.votedFor = data.VotedFor
			c.commitIndex = data.CommitIndex
			c.lastApplied = data.LastApplied
		} else if err != nil {
			if LOG >= INFO {
				c.logger.Printf("Raft %v :Error : While Reading the Metadata file\n", c.pid)
			}
			return false, err
		}
	} else {
		if LOG >= HIGH {
			if isRestart == false {
				c.logger.Printf("Raft %v :Starting\n", c.pid)
			} else {
				c.logger.Printf("Raft %v :  Restart Requested ...But Meta file does not exist\n", c.pid)
			}
		}
		
		// destroying previous log database
		if isRestart == false {
			err = DestroyDatabase(c.dbPath)
			if err != nil {
				return false, err
			}
		}

		file, err := os.Create(c.filePath)
		defer file.Close()
		if err != nil {
			if LOG >= INFO {
				c.logger.Printf("Raft %v :Error : Failed to create file %v \n", c.pid, err)
			}
			return false, err
		}
		c.currentTerm = 0
		c.lStatus.votedFor = -1
		c.commitIndex = 0
		c.lastApplied = 0
		ok, err = c.writeAll(c.currentTerm, c.lStatus.votedFor,c.commitIndex,c.lastApplied)
		if ok == false {
			if LOG >= INFO {
				c.logger.Printf("Raft %v :Error : Error in writing to metadata file %v \n", c.pid, err)
			}
			return ok, err
		}
	}
	// Leader Status
	c.lStatus = leaderStatus{-1, 0 * time.Second, false, 0}

	// added for supporting second phase
	c.outRaft = make(chan interface{}, 100)
	c.inRaft = make(chan *LogItem, 100)
	//c.commitIndex = 0
	//c.lastApplied = 0

	c.dbInterface = GetDBInterface(c.pid, c.logger)

	err = c.dbInterface.OpenConnection(c.dbPath)
	if err != nil {
		if LOG >= INFO {
			c.logger.Printf("Database connection failed %v \n", err)
		}
		return false, err
	}
	
	
	// open connection to KV store 
	c.kvStore, err = kvs.GetKVInterface(kvStorePath) 
	if err != nil {
		if LOG >= INFO {
			c.logger.Printf("Cound not connect rot KV store")
		}
		return false, err
	}
	
	// this would be initialized by leader after each leader election
	c.nextIndex = make(map[int]int64, 0)
	c.matchIndex = make(map[int]int64, 0)

	// update the lastLogIndex
	c.lastLogIndex = c.dbInterface.GetLastLogIndex()
	c.lastLogTerm = c.dbInterface.GetLastLogTerm()

	c.delayChan = make(chan time.Duration, 2)
	c.shutdownChan = make(chan bool, 0)
	rand.Seed(int64(c.pid))
	if LOG >= HIGH {
		c.logger.Printf("Raft %v: Intialion done\n", c.pid)
	}
	c.GetConfiguration()
	return true, nil
}

func (c *consensus) startRaft() {
	var state int
	if LOG >= HIGH {
		c.logger.Printf("Raft %v: Starting Raft Instance  \n ", c.pid)
	}
	// Instance will start in follower state
	state = FOLLOWER // Confirm the case of restart
	timeOutCount = 0
LOOP:
	for {
		switch {
		case state == FOLLOWER:
			state = c.follower()
			if state == FOLLOWER {
				timeOutCount++
			} else {
				timeOutCount = 0
			}
		case state == CANDIDATE:
			state = c.candidate()
			if state == CANDIDATE {
				timeOutCount++
			} else {
				timeOutCount = 0
			}
		case state == LEADER:
			state = c.leader()
		case state == STOP:
			break LOOP
		default:
			if LOG >= INFO {
				c.logger.Printf("Raft %v: Invalid state : %v \n ", state)
				c.logger.Printf("Raft %v: Shutdown  \n ", c.pid)
			}
			break LOOP
		}
	}
	c.shutdownChan <- true
	c.dbInterface.CloseDB()
	c.kvStore.CloseKV()
}

// New method takes the
//	myid : of the local server
// 	path : path of the configuration files
// 	server : serverInstance (set server logger before sending)
// kvStorePath : path of the key value store on the local machine
// isRestart : specify whether it is a restart or fresh start of raft isntance
// The Raft assume that the KVstore exist. In case of the kv store does not exist it
// will throw an error  


func NewRaft(myid int, path string, logLevel int, server *cluster.Server, kvStorePath string,  isRestart bool) (*consensus, bool, error) {
	var c consensus
	c.pid = myid
	LOG = logLevel
	_, err := c.initialize(myid, path, server, kvStorePath,  isRestart)
	if err != nil {
		return nil, false, err
	}
	go c.startRaft()
	if LOG >= INFO {
		c.logger.Println("Raft %v: Successfully Started Raft Instance", c.pid)
	}
	return &c, true, nil
}
