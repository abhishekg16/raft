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


// DEBUG variable will be setup to start server in
//debug which provide the differt kind of facilities
var LOG int

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
	LogMsg 	interface{}
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
		if LOG >= INFO { c.logger.Printf("Raft %v :Error : %v",c.pid, err)	}
		return false, err, persistentData{}
	}
	var data persistentData
	err = json.Unmarshal(msg, &data)
	if err != nil {
		if LOG >= INFO { c.logger.Printf("Raft %v :Error : %v",c.pid, err)	}
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
		if LOG >= INFO {
			c.logger.Println("Raft %v: Error : %v", c.pid ,err)
		}
		return false, err
	}
	file, err := os.OpenFile(c.filePath, os.O_RDWR, 755)
	if err != nil {
		if LOG >= INFO {
			c.logger.Println("Raft %v: Error : %v", c.pid , err)
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

	blockStat blockStatus

	msgId int64
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
	}
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
		c.logger.Printf("Raft %v: Parsing The Configuration File in raft\n",c.pid)
	}
	path = path + "/raft.json"
	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		if LOG >= INFO {
			c.logger.Printf("Raft %v: Parsing Failed %v\n",c.pid, err)
		}
		return false, err
	}
	dec := json.NewDecoder(file)
	for {
		var v map[string]interface{}
		if err := dec.Decode(&v); err == io.EOF || len(v) == 0 {
			if LOG >= FINE {
				c.logger.Printf("Raft %v,Parsing Done !!!\n",c.pid)
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
					c.logger.Printf("Raft %v: Error : Can not retrieve dataDir \n",c.pid)
				}
			}
		} else if prop == "Etimeout" {
			t_timeout, ok := v["Value"].(float64)
			if ok == false {
				if LOG >= INFO {
					c.logger.Printf("Raft %v: Error : Can not retrieve Eelection Timeout \n",c.pid)
				}
			} else { 
				c.eTimeout = time.Duration(t_timeout) * time.Millisecond
			}
		} else if prop == "hfre" {
			t_hfre, ok := v["Value"].(float64)
			if ok == false {
				if LOG >= INFO {
					c.logger.Printf("Raft %v: Error : Can not retrieve href \n",c.pid)
				}
			} else {
				c.heartBeatInterval = time.Duration(t_hfre) * time.Millisecond
			}
		} else if prop == "logDir" {
			c.logDir, ok  = v["Value"].(string) 
			if ok == false {
				if LOG >= INFO {
					c.logger.Printf("Raft %v: Error : Can not retrieve logDir \n",c.pid)
				}
			}
		}
	}
}

// HeartBeats are small messages and which must be send
// TODO : Create a separate port for reciveing the heartbeats
// And the must be processes at the higher priority
// Broadcast Message are not the acknowledged

func (c *consensus) sendHeartBeats( in chan bool, out chan bool ) {
	// TODO : Implement multicast functionality
	heartBeat := AppendEntriesToken{Term: c.currentTerm, LeaderId: c.pid}
	msg := cluster.Message{MsgCode: APPEND_ENTRY, Msg: heartBeat}
	out <- true
	for {
		select {
		case <-time.After(c.heartBeatInterval):
			if LOG >= HIGH {
				c.logger.Println("Heart Beat Manager : Broadcasting HeartBeat")
			}
			c.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgType: cluster.CTRL, Msg: msg}
		case <-in:
			if LOG >= HIGH {
				c.logger.Println("The Beat Manager is being shut down")
			}
			out <- true
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


// leader tracker runs the timer in case timeout happen it will send and message on the out channel
// and will restun
func (c *consensus) leaderTracker(in chan bool, out chan bool) {
	if LOG >= HIGH {	
		c.logger.Printf("Raft %v: Leader Tracker : Got Enable Signal\n",c.pid)
	}
	// Each server will use it's pid as seed to all will have different
	// sequesnce of random values
	// TODO : Timeut for first time must be at lower bound
	randTimeout := c.eTimeout + time.Duration(float64(c.eTimeout.Nanoseconds())*(float64(rand.Intn(10000))/10000))*time.Nanosecond
	if LOG >= FINE  {
		c.logger.Printf("Raft %v: Leader Tracker : Timeout duration %v\n",c.pid ,randTimeout)
	}
	LOOP:
	for {
		select {
			case <-time.After(randTimeout):
				if LOG >= HIGH  {
					c.logger.Println("Raft %v: Leader Tracker : Election Timeout")
				}
				out<-true		
				break LOOP
			case val:= <-in:
				if val == false  {
					if LOG >= HIGH {
						c.logger.Printf("Raft %v: Leader Tracker : Shutdown",c.pid)
					}
					return 
				} 
				continue			
		}
	}
}

func getMessage(env *cluster.Envelope) *cluster.Message {
	tMsg := env.Msg
	return &tMsg
}



// follower method implements the follower state
func (c *consensus) follower() int {
	if LOG >= HIGH {
		c.logger.Printf("Raft %v: Follower State : Follow is Activated\n",c.pid)
	}
	
	in_leaderTracker := make (chan bool, 1)
	out_leaderTracker := make (chan bool, 1)
	go c.leaderTracker(in_leaderTracker,out_leaderTracker)
	
	for {
		select {		
			case env := <-c.server.Inbox():
			if LOG >= FINE { 
				c.logger.Println("Follower State :  message arrived")
			}
			msg := getMessage(env)
			switch {
				case msg.MsgCode == APPEND_ENTRY:
				if LOG >= FINE {
					c.logger.Printf("Follower State : Append Received from %v \n", env.Pid)
				}
				
				data, ok := (msg.Msg).(AppendEntriesToken) 
				if ok == false  {
					if LOG >= INFO  {
						c.logger.Printf("Follower State : Append Entries MssCode with different MsgToken %v \n", env.Pid)
					}	
					continue
				}	 
				term := int64(data.Term) 
				//TODO: Check second condition
				if (c.currentTerm <= term) {
					if LOG >= FINE {
						c.logger.Println("Follower State : Leader Present")
					}
					if c.lStatus.status == true && term == c.currentTerm && c.lStatus.pidOfLeader != data.LeaderId {
						if LOG >= INFO {
							c.logger.Println("Follower State : Error: More then one leader in same term")
						}
						continue
					}  
					// Leader not known yet
					in_leaderTracker <- true
					if  c.lStatus.status == false {
						c.lStatus.status = true 
					}
					if c.lStatus.pidOfLeader != data.LeaderId {
						c.lStatus.pidOfLeader = data.LeaderId								
					}
					if c.currentTerm <  term {
						c.currentTerm = term 
						c.writeTerm(c.currentTerm)
					}
					c.sendNewResponseToken(env.Pid, true )
					/*
					No need to sent the response message if all fine
					*/
				} else {
					if LOG >= FINE {
						c.logger.Println("Follower State : HB with less term Received..sending Negative Reply")
					}
					c.sendNewResponseToken(env.Pid, false)
				}
				case msg.MsgCode == VOTEREQUEST:
					if LOG >= HIGH {
						c.logger.Println("Follower State: Got Vote Request from ", env.Pid)
					}
					data , ok:= (msg.Msg).(VoteRequestToken)
					if ok == false  {
						if LOG >= INFO  {
							c.logger.Printf("Follower State : Append Entries MssCode with different MsgToken %v \n", env.Pid)
						}	
						continue
					}	 
					term := int64(data.Term)
					// TODO : Handle the second vote request coming from the same sender second time
					if c.currentTerm > term {
						if LOG >= HIGH {
							in_leaderTracker <- true	
							c.logger.Println("Follower State : Request With Lower term")
						}
						c.sendNewVoteResponseToken(env.Pid,false)
					} else if c.currentTerm == term && (c.lStatus.votedFor == -1) {
						if LOG >= HIGH {
							c.logger.Println("Follower State : VoteRequest from equal term .. Not voted yet")
							c.logger.Println("Follower State : Giving Vote")
						}
						in_leaderTracker <- true
						c.markVote( term , int(data.CandidateId) )
						c.sendNewVoteResponseToken(env.Pid,true)
					} else if c.currentTerm == term && (c.lStatus.votedFor != -1) {
						if LOG >= HIGH {
							c.logger.Println("Follower State :Vote Request from equal Term , Already Voted")
						}
						if c.lStatus.votedFor == data.CandidateId {
							in_leaderTracker <- true
							c.sendNewVoteResponseToken(env.Pid,true)
						} else {
							c.sendNewVoteResponseToken(env.Pid,false)
						}
					} else {
						if LOG >= HIGH {
							c.logger.Println("Follower State :Vote Request from higher term.. Giveing Vote")
						}
						in_leaderTracker <- true
						c.markVote( term , int(data.CandidateId) )
						c.sendNewVoteResponseToken(env.Pid,true)
					}
				case msg.MsgCode == VOTERESPONSE:
					if LOG >= HIGH  {
						c.logger.Println("Follower State : Got vote response ... rejecting")
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
			}
	}
}


func (c * consensus) markVote( term int64 , candidateId int ) {
	c.currentTerm = term
	c.lStatus.votedFor = candidateId
	c.writeAll(c.currentTerm, c.lStatus.votedFor)
}


func (c* consensus)sendVoteRequestToken(pid int ) {
		voteReq := VoteRequestToken{c.currentTerm, c.pid}
		reqMsg := cluster.Message{MsgCode: VOTEREQUEST, Msg: voteReq}
		msgId := c.getMsgId()
		replyEnv := &cluster.Envelope{Pid: pid, MsgId: msgId , MsgType: cluster.CTRL, Msg: reqMsg}
		c.sendMessage(replyEnv)
}
// follower method implements the candidate state
func (c *consensus) candidate() int {
		if LOG >= HIGH  {
			c.logger.Println("Candidate State : System in candidate state")
		}
		followerPid := c.server.Peers()
		voteResponseMap := make(map[int]bool, len(followerPid))
		voteCount := 1
		c.markVote(c.currentTerm+1,-1)
		c.lStatus.status = false
		majority := (len(followerPid) / 2) + 1
		if LOG >= HIGH {
			c.logger.Printf("Candidate State : Required Majority : %v , Term :%v\n", majority, c.currentTerm)
		}
		
		in_leaderTracker := make (chan bool, 1)
		out_leaderTracker := make (chan bool, 1)
		go c.leaderTracker(in_leaderTracker,out_leaderTracker)
		
		c.sendVoteRequestToken(cluster.BROADCAST)		
		
		for {
			select {		
				case env := <-c.server.Inbox():
					msg := getMessage(env)
					switch {
						case msg.MsgCode == APPEND_ENTRY:
							data ,ok := msg.Msg.(AppendEntriesToken)
							if ok == false {
								if LOG >= INFO {
									c.logger.Println("Candidate State : Message Code and Msgtype Mismatch")
								} 
								continue
							}
							if c.currentTerm <= int64(data.Term) {
								if LOG >= HIGH {
									c.logger.Println("Candidate State : Recived HeatBeat with higher term ==> Leader Elected.")
								}
								in_leaderTracker<-false
								c.lStatus.pidOfLeader = int(data.LeaderId)
								c.lStatus.status = true
								c.currentTerm = int64(data.Term)
								c.markVote(int64(data.Term), int(data.LeaderId) )
								if LOG >= FINE { 
									c.logger.Println("Candidate State : Enabling Follower State")
								}
								c.sendNewResponseToken(env.Pid,true)
								return FOLLOWER
							} else {
								c.logger.Println("Candidate State :Recived HeatBeat with lower Term and rejecting .")
								c.sendNewResponseToken(env.Pid,false)
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
						
						if c.currentTerm > term {
							if LOG >= HIGH {
								c.logger.Println("Candidate State : Vote Request with lower term")
								c.logger.Println("Candidate State : Sending Negative Message")
							}
							c.sendNewVoteResponseToken(env.Pid,false)
						} else if c.currentTerm <= term {
							//TODO : Check the log length
							c.logger.Println("Candidate State : Vote Request with less then or eqaul Term")
							c.logger.Println("Candidate State : Result depend on log length")
							c.markVote(term,int(data.CandidateId))
							c.sendNewVoteResponseToken(env.Pid,true)
							in_leaderTracker <- false
							if LOG >= HIGH {
								c.logger.Println("Candidate State : Enabling Follower State")
							}
							return FOLLOWER
						}
					case msg.MsgCode == VOTERESPONSE:
						data ,ok := (msg.Msg).(VoteResponseToken) 
						if LOG >= INFO  && ok == false{
							c.logger.Println("Candidate State :  MsgCode and Massge token mismatch")
						} 
						if LOG >= HIGH {
							c.logger.Printf("Candidate State : Got a vote Response from %v\n", env.Pid)
						}
						term := int64(data.Term)
						voteGranted := data.VoteGranted
						if term < c.currentTerm {
							if LOG >= HIGH { 
								c.logger.Println("Candidate State : Got a vote Response with lower term")
								c.logger.Println("Candidate State : Rejecting ..Response. ")
							}
						} else if voteGranted == true && c.currentTerm == term {
							_, ok := voteResponseMap[env.Pid]
							if ok == false {
								voteResponseMap[env.Pid] = true
								voteCount++
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
						} else if term > c.currentTerm {
							// TODO : check for the need of the voteGranted == false
							if LOG >= HIGH {
								c.logger.Println("Candidate State : Vote Response from Higher Term.. must be negative")
							}
							c.markVote(term,-1)
							if LOG >= HIGH  { 
								c.logger.Println("Candidate State : Downgrading to follower")
							}
							in_leaderTracker <- false
							return  FOLLOWER
						}
					default:
						c.logger.Println("Case Not handled")
						os.Exit(0)
					}
				case <-out_leaderTracker:
					if LOG >= HIGH  { 
								c.logger.Println("Candidate State : Election Timeout restart Election")
					}
					return CANDIDATE
			}
		}
}

// following method implements the leader state
func (c *consensus) leader() int {
		if LOG >= HIGH  {
			c.logger.Println("Leader State :System in Leader State")
		}
		// TODO : Hoe to ensure that the heart beat is send at higher priority after and interval: Critical to system
		out_heartBeatManager := make (chan bool, 1)
		in_heartBeatManager := make (chan bool, 1)
		go c.sendHeartBeats(in_heartBeatManager, out_heartBeatManager )
		
		c.lStatus.status = true
		c.lStatus.pidOfLeader = c.pid
		for {
			select {
			case env := <-c.server.Inbox():
				msg := getMessage(env)
				switch {
				case msg.MsgCode == APPEND_ENTRY:
					if LOG >= HIGH {
						c.logger.Println("Leader State : Received heart beat")
					}
					data, ok  := (msg.Msg).(AppendEntriesToken)
					if LOG >= INFO && ok == false {
						c.logger.Println("Leader State : Mismatach in MsgCode and MsgToken")
					}
					term := int64(data.Term)
					if c.currentTerm < term {
						if LOG >= INFO {
							c.logger.Println("Leader State : Receive heart beat with higher term")
						}
						c.markVote(term,int(data.LeaderId) ) 
						c.logger.Println("Leader State : Downgrading to follower")
						in_heartBeatManager <- true
						<-out_heartBeatManager
						return FOLLOWER
					} else if c.currentTerm == term {	
						if LOG >= INFO {
							c.logger.Println("Leader State : ERROR : Receive heart beat with equal term")
							c.logger.Println("Leader State : ERROR : Two Leader in same term")
						}
						// check log length
					} else {
						c.logger.Println("Leader State : Receive heart beat with lower term")
						c.sendNewResponseToken(env.Pid,false)
					}
			case msg.MsgCode == RESPONSE:
				data, ok := (msg.Msg).(ResponseToken)
				if ok == false {
					if LOG >= INFO {
						c.logger.Println("Leader State : ERROR : MsgCode and Message token mismatech")
					}
				}
				term := int64(data.Term)
				if c.currentTerm <= term && data.Success == false {
					c.logger.Println("Leader State : Got Negative hB Response")
					c.logger.Println("Leader State : Downgrading to follower")
					in_heartBeatManager <- true
					<- out_heartBeatManager
					return FOLLOWER
				} else {
					//c.logger.Println("Leader State : Got Positive HB Response")
				}

			case msg.MsgCode == VOTEREQUEST:
				if LOG >= HIGH  {
					c.logger.Println("Leader State :Leader received voteRequest")
				}
				data := (msg.Msg).(VoteRequestToken)
				term := int64(data.Term)
				if c.currentTerm > term {
					if LOG >= HIGH {
						c.logger.Println("Leader State : The received request have less term")
						c.logger.Println("Leader State : Sending Negative Reply")
					}
					c.sendNewVoteResponseToken(env.Pid,false)
				} else if c.currentTerm == term {
					if LOG >= HIGH {
						c.logger.Println("Leader State : Vote Request with equal term")
						c.logger.Println("Leader State : Sending Negative Reply")
					}
					c.sendNewVoteResponseToken(env.Pid,false)
				} else {
					if LOG >= INFO {
						c.logger.Println("Leader State : log length will decide")
					}
				}
			case msg.MsgCode == VOTERESPONSE:
				if LOG >= HIGH {
					c.logger.Println("Got vote response.. rejecting")
				}
			default:
				if LOG >= INFO {
					c.logger.Println("Case Not handled")
				}
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

func (c* consensus) sendNewVoteResponseToken(pid int, flag bool) {
	replyMsg := c.getNewVoteResponseToken(false)
	//msgId = c.getMsgId()
	replyEnv := &cluster.Envelope{Pid: pid, MsgType: cluster.CTRL, Msg: *replyMsg}
	c.sendMessage(replyEnv)
}

func (c *consensus) getNewResponseToken(flag bool) *cluster.Message {
	vote := ResponseToken{c.currentTerm, flag}
	replyMsg := cluster.Message{MsgCode: RESPONSE, Msg: vote}
	return &replyMsg
}

func ( c * consensus)sendNewResponseToken( pid int , flag bool ) {
	replyMsg := c.getNewResponseToken(flag)
	//msgId = c.getMsgId()
	replyEnv := &cluster.Envelope{Pid: pid ,MsgType: cluster.CTRL, Msg: *replyMsg}
	c.sendMessage(replyEnv)
}

// initialize the raft instance
func (c *consensus) initialize(pid int, path string, server *cluster.Server, isRestart bool) (bool, error) {
	
	logFileName := c.logDir + "/" + LOGFILENAME + strconv.Itoa(c.pid)
	// TODO add Method to check the existance of log file 
	file, err := os.Create(logFileName)
	if err != nil {
		if LOG >= INFO  {	
		log.Printf("Raft %v: Error : %v\n",c.pid, err)	
		}
	}
	//c.logger = log.New(file, "Log: ", log.Ldate|log.Ltime|log.Lshortfile)
	c.logger = log.New(file, "Log: ", log.Ltime|log.Lshortfile)
	
	if LOG >= HIGH  { 		
		c.logger.Printf("Raft %v: Intializing the Raft , Restart : %v\n", pid, isRestart )	
	}

	ok, err := c.parse(pid, path)
	if ok == false { 
		if (LOG >= INFO ) {
			c.logger.Printf("Raft %v: Error : Parsing Failed, %v\n", c.pid, err)
		}
		return false, err
	}
	
	c.server = *server
	c.GetConfiguration()

	//TODO :Chech this condition
	// Each of the server will start as the follower, or it sould be previously failed condition
	c.state = FOLLOWER
	c.filePath = c.dataDir + "/" + FILENAME + strconv.Itoa(c.pid)
	
	// Check if file exist from previos failure
	if _, err := os.Stat(c.filePath); (!isRestart) &&  os.IsExist(err) {
		if (LOG >= HIGH ) {  
			c.logger.Printf("Raft %v :Restarting\n",c.pid) 
		}
		if (LOG >= HIGH ) {
			c.logger.Printf("Raft %v :Meta data file already exist\n",c.pid) 
		}
		ok, err, data := c.readDisk()
		if ok == true {
			c.currentTerm = data.Term
			c.lStatus.votedFor = data.VotedFor
		} else if err != nil {
			if (LOG >= INFO ) {
				c.logger.Printf("Raft %v :Error : While Reading the Metadata file\n",c.pid) 
			}
			return false, err
		}
	} else {
		if (LOG >= HIGH ) { 
			if  isRestart == false {  
				c.logger.Printf("Raft %v :Starting\n",c.pid)
			} else {
				c.logger.Printf("Raft %v :Meta file does not exist\n",c.pid)
			}
		}
		file, err := os.Create(c.filePath)
		defer file.Close()
		if err != nil {
			if LOG >= INFO {
				c.logger.Printf("Raft %v :Error : Failed to create file %v \n",c.pid, err)
			}
			return false, err
		}
		c.currentTerm = 0
		c.lStatus.votedFor = -1
		ok, err = c.writeAll(c.currentTerm, c.lStatus.votedFor)
		if ok == false {
			if LOG >= INFO {
				c.logger.Printf("Raft %v :Error : Error in writing to metadata file %v \n",c.pid, err)
			}
			return ok, err
		}
	}
	// Leader Status
	c.lStatus = leaderStatus{-1, 0 * time.Second, false, 0}
	
	c.blockStat.set(false)
	rand.Seed(int64(c.pid))
	if LOG >= HIGH  { 		
		c.logger.Printf("Raft %v: Intialion done\n", c.pid, )	
	}
	return true, nil
}


func (c* consensus)startRaft() {
	var state int
	if LOG >= HIGH {
		c.logger.Printf("Raft %v: Starting Raft Instance  \n " ,c.pid)
	}
	// Instance will start in follower state 
	state = FOLLOWER  // Confirm the case of restart
	LOOP:
	for {
		switch {
			case state == FOLLOWER:
				state = c.follower()
			case state == CANDIDATE: 
				state = c.candidate()
			case state == LEADER:
				state = c.leader()
			default :
			if LOG >= INFO  {
				c.logger.Printf("Raft %v: Invalid state : %v \n " ,state )
				c.logger.Printf("Raft %v: Shutdown  \n ",c.pid)
			}	
			break LOOP	
		}
	}
}

// New method takes the
//	myid : of the local server
//  path : path of the configuration files
//  server : serverInstance (set server logger before sending)

func NewRaft(myid int, path string, logLevel int, server *cluster.Server,isRestart bool) (*consensus, bool, error) {
	var c consensus
	c.pid = myid
	LOG = logLevel
	_, err := c.initialize(myid, path,server,isRestart)
	if err != nil {
		return nil, false, err
	}
	go c.startRaft()
	if LOG >= INFO {
		c.logger.Println("Raft %v: Successfully Started Raft Instance",c.pid)
	} 
	return &c, true, nil
}
