package raft

/*
Raft package implement the raft consensus algorithm. This algorithm is used for making concensus in
distributed system. The algorithm gaurantee that there would be only one leader a time.
This implementation runs three go routine one for rach leader , follower and candidate states.
On the basis of the incoming signal an the timer events the consensus algorithm's state
is switched between these threds
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
	//"fmt"
	"encoding/gob"
	"math/rand"
	//	"bufio"
	"strconv"
)

// FILENAME is the name of the file which will store the persistent data
// related to the current raft instance

const (
	FILENAME    = "book"
	LOGFILENAME = "log"
)

// These constant will identify the types of the Messages s
const (
	HEARTBEAT    = iota // Repesent the heart beat message
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
)

// DEBUG variable will be setup to start server in
//debug which provide the differt kind of facilities
var DEBUG int

// This struct keeps the details of the blocking request
type blockStatus struct {
	isBlock  bool
	bLock    sync.Mutex
	duration time.Duration
}

// return the true if blocking is true
func (b *blockStatus) get() bool {
	defer b.bLock.Unlock()
	b.bLock.Lock()
	return b.isBlock
}

// set blocking true
func (b *blockStatus) set(val bool) {
	defer b.bLock.Unlock()
	b.bLock.Lock()
	b.isBlock = val
}

//
func (b *blockStatus) setDuration(d time.Duration) {
	b.duration = d
}

func (b *blockStatus) getDuration() time.Duration {
	return b.duration
}

// This Token would be used for AppendEntries/HeartBeat Token
// Even though the Message Code would be able to identify the Actual Purpose of the Token

type AppendEntriesToken struct {
	Term     int64
	LeaderId int
}

// Response Token would be used for response of the AppendEntry/HeartBeat Token
type ResponseToken struct {
	Term    int64
	Success bool
}

// VoteRequestToekn would be used to request the vote
type VoteRequestToken struct {
	Term        int64
	CandidateId int
	//	LastLogIndex int64
	//	LstLogTerm int64
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
}

// This method reads the last persistent data data on the disk
func (c consensus) readDisk() (bool, error, persistentData) {
	msg, err := ioutil.ReadFile(c.filePath)
	if err != nil {
		return false, err, persistentData{}
	}
	var data persistentData
	err = json.Unmarshal(msg, &data)
	if err != nil {
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
	c.writeAll(currentTerm, data.VotedFor)
	return true, nil
}

// this method write the voteFor variable on the disk
func (c consensus) writeVotedFor(votedFor int) (bool, error) {
	ok, err, data := c.readDisk()
	if ok == false {
		return ok, err
	}
	c.writeAll(data.Term, votedFor)
	return true, nil
}

func (c consensus) writeAll(currentTerm int64, votedFor int) (bool, error) {
	data := persistentData{Term: currentTerm, VotedFor: votedFor}
	msg, err := json.Marshal(data)
	if err != nil {
		c.logger.Println("Error in wrting data to disk")
		return false, err
	}
	file, err := os.OpenFile(c.filePath, os.O_RDWR, 755)
	if err != nil {
		return false, err
	}
	defer file.Close()
	file.Truncate(0)
	n, err := file.Write(msg)
	// TODO : Check the case when the complete file can not be written
	// CHECK : whthe rthe write operation is atomic or not
	if n < len(msg) || err != nil {
		c.logger.Println("Error Occured in WriteAll method")
		return false, err
	}
	return true, err
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

	resumeLeaderTracker chan bool

	in_sig_heartBeatManager chan bool

	out_sig_heartBeatManager chan bool

	stateLock sync.Mutex // This mutex lock is to update the role of the system

	filePath string // RecordFile which store the persistent data

	dataDir string

	logDir string // path of the log files

	logger *log.Logger // logger this raft instance

	mutexOutbox sync.Mutex // mutex fox server out channel
	mutexInbox  sync.Mutex // mutex lock for server in channel

	// in channel for leader, follower and candidate
	in_follower  chan bool
	in_leader    chan bool
	in_candidate chan bool

	// enabling channel for leader tracker
	enableLeaderTracker chan bool
	// Signal channel used by leader trackier to sen t5he signals
	leaderTrackerSignal chan bool
	// input channel for leader tracker
	in_leaderTracker chan bool

	electionTimeoutState bool

	electionTimeoutMutex sync.Mutex

	enableTeaderTracker chan bool

	blockStat blockStatus

	isShutdownSignal bool // to signal to shutdown itself
	// all go rotuine will check for this
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

/*
func (c * cencensus) Block(duration time.Duration) {
	if (DEBUG == 0)
		return
	c.blockStat.setDuration(duration)
	c.blockStat.set(true)
}
*/

// Shutdown stops all the threds
func (c *consensus) Shutdown() {
	c.logger.Printf("Raft %v, Shutdown is called", c.pid)
	c.server.Shutdown()
	c.isShutdownSignal = true
}

// Print the Configuration of the raft instance
func (c *consensus) GetConfiguration() {
	c.logger.Println("============= CONFIGURATION =================")
	c.logger.Printf("\t PID : %v \n", c.pid)
	c.logger.Printf("\t ELECTION TIMEOUT : %v \n", c.eTimeout)
	c.logger.Printf("\t DATA DIR : %v \n", c.dataDir)
	c.logger.Printf("\t HEART BEAT INTERVAL %v \n", c.heartBeatInterval)
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
	log.Println("Parsing The Configuration File in raft")
	path = path + "/raft.json"
	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		log.Printf("Parsing Failed %q", err)
		return false, err
	}
	dec := json.NewDecoder(file)
	for {
		var v map[string]interface{}
		if err := dec.Decode(&v); err == io.EOF || len(v) == 0 {
			log.Println("Parsing Done !!!")
			return true, nil
		} else if err != nil {
			return false, err
		}
		prop := v["property"]
		/*
			if prop == "myid" {
				t_pid := v["Value"].(float64)
				c.pid = int(t_pid)
			} else */
		if prop == "dataDir" {
			c.dataDir = v["Value"].(string)
		} else if prop == "Etimeout" {
			t_timeout := v["Value"].(float64)
			c.eTimeout = time.Duration(t_timeout) * time.Millisecond
		} else if prop == "hfre" {
			t_hfre := v["Value"].(float64)
			c.heartBeatInterval = time.Duration(t_hfre) * time.Millisecond
		} else if prop == "logDir" {

			c.logDir = v["Value"].(string)
		}
	}
}

// HeartBeats are small messages and which must be send
// TODO : Create a separate port for reciveing the heartbeats
// And the must be processes at the higher priority
// Broadcast Message are not the acknowledged

func (c *consensus) sendHeartBeats() {
	// TODO : Implement multicast functionality
	heartBeat := AppendEntriesToken{Term: c.currentTerm, LeaderId: c.pid}
	msg := cluster.Message{MsgCode: HEARTBEAT, Msg: heartBeat}
	c.out_sig_heartBeatManager <- true
	for {
		select {
		case <-time.After(c.heartBeatInterval):
			c.logger.Println("Heart Beat Manager : Broadcasting HeartBeat")
			c.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgType: cluster.CTRL, Msg: msg}
		case <-c.in_sig_heartBeatManager:
			c.logger.Println("The Beat Manager is being shut down")
			c.out_sig_heartBeatManager <- true
			return
		}
	}
}

func init() {
	log.Println("Registering Interfaces in gob")
	gob.Register(VoteRequestToken{})
	gob.Register(VoteResponseToken{})
	gob.Register(AppendEntriesToken{})
	gob.Register(ResponseToken{})

}

// leaderTracker keepa nd eye on leader
// case did not heard from leader raise alarm
func (c *consensus) leaderTracker() {
	for {
		c.logger.Println("Leader Tracker : waiting for enable signal")
		<-c.enableLeaderTracker
		c.logger.Println("Leader Tracker : Got Enable Signal")
		c.electionTimeoutMutex.Lock()
		c.electionTimeoutState = false
		c.electionTimeoutMutex.Unlock()
		c.logger.Println("Leader Tracker : Started , Sending back notification")
		c.leaderTrackerSignal <- true
		// Each server will use it's pid as seed to all will have different
		// sequesnce of random values
		// TODO : Timeut for first time must be at lower bound
		randTimeout := c.eTimeout + time.Duration(float64(c.eTimeout.Nanoseconds())*(float64(rand.Intn(10000))/10000))*time.Nanosecond
		c.logger.Printf("Timeout duration %v\n", randTimeout)
	LOOP:
		for {
			// only check when leader tracker is running
			if c.isShutdownSignal == true {
				c.logger.Printf("Leader Tracker Thread: SHUTDOWN \n")
				return
			}
			select {
			case <-time.After(randTimeout):
				c.logger.Println("Leader Tracker : Election Timeout")
				c.electionTimeoutMutex.Lock()
				c.electionTimeoutState = true
				c.electionTimeoutMutex.Unlock()
				break LOOP
			case <-c.in_leaderTracker:
				continue
			}
		}
	}
}

func getMessage(env *cluster.Envelope) *cluster.Message {
	tMsg := env.Msg
	return &tMsg
}

// follower method implements the follower state
func (c *consensus) follower() {
	for {
		<-c.in_follower
		c.logger.Println("Follower State : Checking Leader tacker state")
		c.electionTimeoutMutex.Lock()
		electionStatus := c.electionTimeoutState
		c.electionTimeoutMutex.Unlock()
		// TODO : One canissing when time out just got false after check
		if electionStatus == true {
			c.logger.Println("Follower State : Leader Tracker is blocked, sending enable signal ")
			c.enableLeaderTracker <- true
			<-c.leaderTrackerSignal
		}
		// Leader tracker would be active and automatically go down
		if c.isShutdownSignal == true && c.state != FOLLOWER {
			c.logger.Printf("FOLLOWER THREAD %v: SHUTDOWN\n", c.pid)
			return
		}

		if c.state != FOLLOWER {
			c.state = FOLLOWER
		}
		c.logger.Println("Follower State : Follow is Activated")
	LOOP:
		for {

			// TODO : REMOVE BUSY WAITING
			c.electionTimeoutMutex.Lock()
			electionStatus := c.electionTimeoutState
			c.electionTimeoutMutex.Unlock()
			c.logger.Printf("Follower State : election status : %v \n", electionStatus)
			if electionStatus == true {
				c.logger.Println("Follower State : Election Timeout")
				c.logger.Println("Follower State : Moving to Candidate")
				c.in_candidate <- true
				c.state = CANDIDATE // TO Be removed
				break LOOP
			}
			if c.isShutdownSignal == true {
				// because election time out not happended leader tracker s working
				// so that will auto matically go down
				c.logger.Printf(" Follower State %v: SHUTDOWN CALLED\n", c.pid)
				c.logger.Printf(" Follower State %v: Sending Request to leader and candidate to go down\n", c.pid)
				c.in_leader <- true
				c.in_candidate <- true
				c.logger.Printf(" Follower Thread %v: SHUTDOWN\n", c.pid)
				return
			}

			env := c.inbox()
			if env == nil {
				c.logger.Println("Follower State : No message arrived")
				continue
			}
			c.logger.Println("Follower State :  message arrived")
			msg := getMessage(env)
			switch {
			case msg.MsgCode == HEARTBEAT:
				c.logger.Printf("Follower State : HeartBeat Received from %q \n", env.Pid)
				data := (msg.Msg).(AppendEntriesToken)
				term := int64(data.Term)
				//TODO: Check second condition
				if (c.currentTerm == term && env.Pid == c.lStatus.pidOfLeader) || c.lStatus.status == false {
					c.in_leaderTracker <- true
					if c.lStatus.status == false {
						c.lStatus.status = true
						c.lStatus.pidOfLeader = env.Pid
					}
					c.logger.Println("Follower State : LeaderIsActive")
					replyMsg := c.getNewResponseToken(true)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.sendMessage(replyEnv)
				} else if c.currentTerm == term && env.Pid != c.lStatus.pidOfLeader {
					c.logger.Println("Follower State : Master has been changed.....never possible")
					c.lStatus.pidOfLeader = env.Pid
					c.logger.Println("Follower State : New master is %q", c.lStatus.pidOfLeader)
				} else if c.currentTerm < term {
					c.logger.Println("Follower State : Higher Term HB Received")
					c.in_leaderTracker <- true
					c.currentTerm = term
					//TODO : check if need to store voted for the leader
					c.writeTerm(c.currentTerm)
					if c.lStatus.status == false {
						c.lStatus.status = true
						c.lStatus.pidOfLeader = env.Pid
					}
					replyMsg := c.getNewResponseToken(true)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.sendMessage(replyEnv)
				} else {
					c.logger.Println("Follower State : HB with less term Received")
					replyMsg := c.getNewResponseToken(false)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.sendMessage(replyEnv)
				}
			case msg.MsgCode == VOTEREQUEST:
				c.logger.Println("Follower State: Got Vote Request from ", env.Pid)
				data := (msg.Msg).(VoteRequestToken)
				term := int64(data.Term)
				// TODO : Handle the second vote request coming from the same sender second time
				if c.currentTerm > term {
					c.logger.Println("Follower State : Request With Lower term")
					replyMsg := c.getNewVoteResponseToken(false)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.sendMessage(replyEnv)
					// TODO : Fix c.lStatus.votedFor = should not be equal to self id
				} else if c.currentTerm == term && (c.lStatus.votedFor == -1 || c.lStatus.votedFor == c.pid) {
					c.logger.Println("Follower State : Current Term eqaul to vote Request")
					c.logger.Println("Follower State : Not voted for any one")
					c.in_leaderTracker <- true
					cid := int(data.CandidateId)
					c.lStatus.votedFor = cid
					c.writeAll(c.currentTerm, c.lStatus.votedFor)
					replyMsg := c.getNewVoteResponseToken(true)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.logger.Println("Follower State : Giving Vote")
					c.sendMessage(replyEnv)
				} else {
					c.logger.Println("Follower State : Request Vote received with higher term")
					c.in_leaderTracker <- true
					c.currentTerm = term
					cid := int(data.CandidateId)
					c.lStatus.votedFor = cid
					c.writeAll(c.currentTerm, c.lStatus.votedFor)
					replyMsg := c.getNewVoteResponseToken(true)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.logger.Println("Follower State :Giving Vote")
					c.sendMessage(replyEnv)
				}
			case msg.MsgCode == VOTERESPONSE:
				c.logger.Println("Follower State : Got vote response ... rejecting")

			case msg.MsgCode == RESPONSE:
				c.logger.Println("Follower State : Got HB response ... rejecting")
			default:
				c.logger.Println("Case Not handled")
				os.Exit(0)
			}
		}
	}
}

const (
	STARTELECTION = iota
	GOTVOTE       = iota
	STOP          = iota
	STOPPED       = iota
)

// follower method implements the candidate state
func (c *consensus) candidate() {
	for {
		<-c.in_candidate

		c.logger.Println("Candidate State : System in candidate state")
		followerPid := c.server.Peers()
		voteResponseMap := make(map[int]bool, len(followerPid))
		voteCount := 1
		c.currentTerm++
		c.lStatus.votedFor = -1
		c.lStatus.status = false
		c.writeAll(c.currentTerm, c.lStatus.votedFor)
		c.logger.Println("Candidate State : Incremented The term , Voted for self")
		majority := (len(followerPid) / 2) + 1
		c.logger.Printf("Candidate State : Required Majority : %v\n", majority)
		c.logger.Printf("Candidate State : Starting Election With Term %v\n", c.currentTerm)
		c.enableLeaderTracker <- true
		c.logger.Printf("Candidate State : Enabled Leader Tracker %v\n", c.currentTerm)
		<-c.leaderTrackerSignal

		if c.isShutdownSignal == true && c.state != CANDIDATE {
			c.logger.Printf("Candidate Thread %v: SHUTDOWN \n", c.pid)
			return
		}
		if c.state != CANDIDATE {
			c.state = CANDIDATE
		}
		// TODO : Make sure that the all the message are send at high priority
		voteReq := VoteRequestToken{c.currentTerm, c.pid}
		reqMsg := cluster.Message{MsgCode: VOTEREQUEST, Msg: voteReq}
		replyEnv := &cluster.Envelope{Pid: cluster.BROADCAST, MsgType: cluster.CTRL, Msg: reqMsg}
		c.sendMessage(replyEnv)
	LOOP:
		for {
			c.electionTimeoutMutex.Lock()
			electionStatus := c.electionTimeoutState
			c.electionTimeoutMutex.Unlock()
			if electionStatus == true {
				c.logger.Println("Candidate State : Election Timeout")
				c.in_candidate <- true
				break LOOP
			}
			if c.isShutdownSignal == true {
				// surely leader tracker is running
				c.logger.Printf("Candidate State %v: Shutdown signal\n ", c.pid)
				c.logger.Printf("Candidate State %v: Sendind shutdown to leader and follower\n", c.pid)
				c.in_follower <- true
				c.in_leader <- true
				c.logger.Printf("Candidate Thread %v: SHUTDOWN", c.pid)
				return
			}

			env := c.inbox()
			if env == nil {
				c.logger.Println("Candidate State : No message arrived")
				continue
			}
			msg := getMessage(env)
			switch {
			case msg.MsgCode == HEARTBEAT:
				data := msg.Msg.(AppendEntriesToken)
				if c.currentTerm <= int64(data.Term) {
					c.logger.Println("Candidate State : Recived HeatBeat with higher term ==> Leader Elected.")
					c.in_leaderTracker <- true
					c.lStatus.pidOfLeader = int(data.LeaderId)
					c.lStatus.status = true
					c.lStatus.votedFor = int(data.LeaderId)
					c.currentTerm = int64(data.Term)
					c.writeAll(c.currentTerm, c.lStatus.votedFor)
					c.state = FOLLOWER
					c.in_leaderTracker <- true
					c.in_follower <- true
					c.logger.Println("Candidate State : Enabling Follower State")
					break LOOP
				} else {
					c.logger.Println("Candidate State :Recived HeatBeat with lower Term and rejecting .")
					replyMsg := c.getNewResponseToken(false)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.sendMessage(replyEnv)
				}
			case msg.MsgCode == VOTEREQUEST:
				c.logger.Println("Candidate State : Vote Request Recieved")
				data := (msg.Msg).(VoteRequestToken)
				term := int64(data.Term)
				if c.currentTerm > term {
					c.logger.Println("Candidate State : Vote Request with lower term")
					replyMsg := c.getNewVoteResponseToken(false)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.logger.Println("Candidate State : Sending Negative Message")
					c.sendMessage(replyEnv)
				} else if c.currentTerm <= term {
					//TODO : Check the log length
					c.logger.Println("Candidate State : Vote Request with less then or eqaul Term")
					c.logger.Println("Candidate State : Result depend on log length")
					c.currentTerm = term
					c.lStatus.votedFor = int(data.CandidateId)
					c.writeAll(c.currentTerm, c.lStatus.votedFor)
					replyMsg := c.getNewVoteResponseToken(true)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.sendMessage(replyEnv)
					c.state = FOLLOWER
					c.in_leaderTracker <- true
					c.in_follower <- true
					c.logger.Println("Candidate State : Enabling Follower State")
					break LOOP
				}
			case msg.MsgCode == VOTERESPONSE:
				data := (msg.Msg).(VoteResponseToken)
				c.logger.Printf("Candidate State : Got a vote Response from %v\n", env.Pid)
				term := int64(data.Term)
				voteGranted := data.VoteGranted
				if term < c.currentTerm {
					//c.logger.Println("Candidate State : Got a vote Response ")
					c.logger.Println("Candidate State : Rejecting ..Response. ")
				} else if voteGranted == true && c.currentTerm == term {
					_, ok := voteResponseMap[env.Pid]
					if ok == false {
						voteResponseMap[env.Pid] = true
						voteCount++
						c.logger.Printf("Candidate State : Vote Count %v \n ", voteCount)
					}
					if voteCount >= majority {
						// majority achieved
						c.logger.Println(c.pid, " Candidate State : has been elected as a leader")
						c.lStatus.pidOfLeader = c.pid
						c.lStatus.status = true
						c.state = LEADER
						c.in_leader <- true
						break LOOP
					}
				} else if term > c.currentTerm {
					// down grade to follower
					c.logger.Println("Candidate State : Vote Response from Higher Term.. must be negative")
					c.currentTerm = term
					c.lStatus.votedFor = -1
					c.writeAll(c.currentTerm, c.lStatus.votedFor)
					c.logger.Println("Candidate State : Downgrading to follower")
					c.state = FOLLOWER
					c.in_leaderTracker <- true
					c.in_follower <- true
					break LOOP
				}
			default:
				c.logger.Println("Case Not handled")
				os.Exit(0)
			}
		}
	}
}

// follower method implements the leader state
func (c *consensus) leader() {
	for {
		<-c.in_leader
		if c.isShutdownSignal == true && c.state != LEADER {
			c.logger.Printf("LEADER THREAD %v: SHUTDOWN\n", c.pid)
			return
		}
		if c.state != LEADER {
			c.state = LEADER
		}
		c.logger.Println("Leader State :System in Leader State")
		// TODO : Hoe to ensure that the heart beat is send at higher priority after and interval: Critical to system
		go c.sendHeartBeats()
		<-c.out_sig_heartBeatManager

		c.lStatus.status = true
		c.lStatus.pidOfLeader = c.pid

	LOOP:
		for {
			if c.isShutdownSignal == true {
				c.logger.Printf("Leader State %v: Shutdown recieved\n", c.pid)
				c.logger.Printf("Leader State %v: Requesting HB to shutdown\n", c.pid)
				c.in_sig_heartBeatManager <- true // stop HB MANAGER
				<-c.out_sig_heartBeatManager
				c.electionTimeoutMutex.Lock()
				electionStatus := c.electionTimeoutState
				c.electionTimeoutMutex.Unlock()
				c.logger.Printf("Leader State %v: Requesting Leader Tracker to shutdown\n", c.pid)
				if electionStatus == true {
					c.in_leaderTracker <- true // Send signal to LT can be reminated
					c.leaderTrackerSignal <- true
				}
				c.logger.Printf("Leader State %v: Requesting Follower and candidate to shutdown\n", c.pid)
				c.in_follower <- true  //follower will go down
				c.in_candidate <- true // candidate will go down
				c.logger.Printf("LEADER THREAD %v: SHUTDOWN\n", c.pid)
				return

			}
			env := c.inbox()
			if env == nil {
				c.logger.Println("Leader State : No message arrived")
				continue
			}
			msg := getMessage(env)
			switch {
			case msg.MsgCode == HEARTBEAT:
				c.logger.Println("Leader State : Received heart beat")
				data := (msg.Msg).(AppendEntriesToken)
				term := int64(data.Term)
				if c.currentTerm < term {
					c.logger.Println("Leader State : Receive heart beat with higher term")
					c.currentTerm = term
					c.lStatus.votedFor = int(data.LeaderId)
					c.writeAll(c.currentTerm, c.lStatus.votedFor)
					c.logger.Println("Leader State : Downgrading to follower")
					c.in_sig_heartBeatManager <- true
					<-c.out_sig_heartBeatManager
					c.state = FOLLOWER
					c.in_leaderTracker <- true
					c.in_follower <- true
					break LOOP
				} else if c.currentTerm == term {
					c.logger.Println("Leader State : Receive heart beat with equal term")
					c.logger.Println("Leader got the heart beat with eqaul term log length will decide")
					// check log length
				} else {
					c.logger.Println("Leader State : Receive heart beat with lower term")
					replyMsg := c.getNewResponseToken(false)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.sendMessage(replyEnv)
				}
			case msg.MsgCode == RESPONSE:
				data := (msg.Msg).(ResponseToken)
				term := int64(data.Term)
				if c.currentTerm <= term && data.Success == false {
					c.logger.Println("Leader State : Got Negative hB Response")
					c.logger.Println("Leader State : Downgrading to follower")
					c.in_sig_heartBeatManager <- true
					<-c.out_sig_heartBeatManager
					c.state = FOLLOWER
					c.in_leaderTracker <- true
					c.in_follower <- true
				} else {
					//c.logger.Println("Leader State : Got Positive HB Response")
				}

			case msg.MsgCode == VOTEREQUEST:
				c.logger.Println("Leader State :Leader received voteRequest")
				data := (msg.Msg).(VoteRequestToken)
				term := int64(data.Term)
				if c.currentTerm > term {
					c.logger.Println("Leader State : The received request have less term")
					replyMsg := c.getNewVoteResponseToken(false)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.logger.Println("Leader State : Sending Negative Reply")
					c.sendMessage(replyEnv)
				} else if c.currentTerm == term {
					c.logger.Println("Leader State : Vote Request with equal term")
					replyMsg := c.getNewVoteResponseToken(false)
					replyEnv := &cluster.Envelope{Pid: env.Pid, MsgType: cluster.CTRL, Msg: *replyMsg}
					c.logger.Println("Leader State : Sending Negative Reply")
					c.sendMessage(replyEnv)
				} else {
					c.logger.Println("log length will decide")
				}
			case msg.MsgCode == VOTERESPONSE:
				c.logger.Println("Got vote response.. rejecting")
			default:
				c.logger.Println("Case Not handled")
				os.Exit(0)
			}
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

func (c *consensus) getNewResponseToken(flag bool) *cluster.Message {
	vote := ResponseToken{c.currentTerm, flag}
	replyMsg := cluster.Message{MsgCode: RESPONSE, Msg: vote}
	return &replyMsg
}

// initialize the raft instance
func (c *consensus) initialize(pid int, path string) (bool, error) {
	ok, err := c.parse(pid, path)
	if !ok {
		log.Println(err)
		return false, err
	}

	if err != nil {
		log.Println("Error in instantiatind the Server Instance")
		return false, err
	}

	logFileName := c.logDir + "/" + LOGFILENAME + strconv.Itoa(c.pid)
	log.Println("logfilename :", logFileName)

	file, err := os.Create(logFileName)
	if err != nil {
		log.Fatalln("Failed to open log file", err)
	}
	c.logger = log.New(file, "Log: ", log.Ldate|log.Ltime|log.Lshortfile)

	// set Logger for underlying server
	//c.server.SetLogger(c.logger)

	sPath := path + "/servers.json"
	c.server, err = cluster.New(pid, sPath, c.logger)

	c.GetConfiguration()

	//TODO :Chech this condition
	// Each of the server will start as the follower, or it sould be previously failed condition
	c.state = FOLLOWER
	c.filePath = c.dataDir + "/" + FILENAME + strconv.Itoa(c.pid)

	// TODO: Add condition to reset the server in which case it will not read the previous file
	// Check if file exist from previos failure
	if _, err := os.Stat(c.filePath); os.IsExist(err) {
		log.Println("Meta data file already exist")
		ok, err, data := c.readDisk()
		if ok == true {
			c.currentTerm = data.Term
			c.lStatus.votedFor = data.VotedFor
		} else if err != nil {
			return false, err
		}
	} else {
		log.Println("Meta data file doen not exist")
		file, err := os.Create(c.filePath)
		if err != nil {
			log.Println("Initialization : Count not Create New File")
			return false, err
		}
		file.Close()
		c.currentTerm = 0
		c.lStatus.votedFor = -1
		ok, err = c.writeAll(c.currentTerm, c.lStatus.votedFor)
		if ok == false {
			return ok, err
		}
	}

	// follower channel
	c.in_follower = make(chan bool, 1)
	c.in_leader = make(chan bool, 1)
	c.in_candidate = make(chan bool, 1)
	c.enableLeaderTracker = make(chan bool, 1)
	c.leaderTrackerSignal = make(chan bool, 1)

	// this chan should be blocking because leader tracker is most important thread in the system
	c.in_leaderTracker = make(chan bool, 3)

	// Special channel to enable the LeaderTracker
	c.resumeLeaderTracker = make(chan bool, 1)

	// Leader Status
	c.lStatus = leaderStatus{-1, 0 * time.Second, false, 0}

	// Channels for heart beat manager
	c.in_sig_heartBeatManager = make(chan bool, 1)
	c.out_sig_heartBeatManager = make(chan bool, 1)

	// Making Leader tracker disabled
	// So it would be enabled by the state
	c.electionTimeoutState = true

	c.blockStat.set(false)
	rand.Seed(int64(c.pid))

	c.isShutdownSignal = false

	return true, nil
}

// New method takes the
//	myid : of the local server
//  path : path of the configuration files
//  mode : specify the mnode of start
//			0 => normal mode
//			1 => Debug
//          2 => Debug which logs
func NewRaft(myid int, path string, mode int) (*consensus, bool, error) {

	var c consensus
	c.pid = myid
	_, err := c.initialize(myid, path)
	if err != nil {
		return nil, false, err
	}
	DEBUG = mode
	go c.leader()
	go c.follower()
	go c.candidate()
	go c.leaderTracker()
	c.in_follower <- true
	return &c, true, nil
}
