#Raft (Consensus Module)#
This project provides a highly reliable Key-Value store. It uses the Raft consensus algorithm to  keep all the instances of the K-V store consistent. 
The consensus algorithm guarantee that there would be only one leader a time in the system for a term. The client can contact to leader and get there command executed on the K-V store. 
Current Implementation has one drawback the raft layer itself updates the K-V store which in not a good design. I might fix this issue in future.
The raft provides the well defined interface by using which you can get information about the leader, term etc. Raft layer uses the cluster implementation as a underlying communication channel which allows different raft instances to communicate over the network. The underlying communication layer give best effort to deliver message but does not guarantee to deliver it.
	
It uses LevelDB , which is key-Value store to store the log entries at persistent store. 


####Features of Implementation ####
1. Provides easily configurable files 
2. The Raft layer take storage and server instances as parameter(Dependency injection which allows low coupling)
3. File logging facilities
4. System can work fine in case of lost message, Delayed message.
5. System can bear upto n/2 node failures.
6. Provide two different port for the message and control message (But not used in current Implementation)
7. Ensure the idempotent execution of the command, ensures each command is executed only once
8. Provide APIs which allows to system to integrate any Key-Value store
9. Provides linearizable semantic.

#### Limitation ####
1. The storage is linked with the replication layer which is a bad design. But Raft layer uses the in interface to communicate with storage layer. So
in order to integrate a new storage use can rewrite that interface.
2. The read commands are also replicated in log which increases the overhead.


##Usages##
Raft provides a set of configuration files which can be easily configured to control the system behavior. All the configuration files are present in the conf folder. These files allows user to set proper values of the heart beat frequency, election timeout, log storage locations etc. Configuration files also includes the enough description about each configuration parameter. 

##### raft.json 
raft.json have all the configuration parameters related to the raft instance.

1. myid : Id of the raft instance 
2. Etimeout : Election timeout
2. hfre : Time interval between the heartbeat signals
3. dataDir : directory where all the persistent data related to raft is stored
4. logDir : directory where all the log message are dumped


##### servers.json
servers.json file have all the configuration parameter related to the cluster instance. It contains the IDs, the socket address of all the peer instances. ID : represents the ID for the server.(should start from 0), IP : IP address of the server instance, Port1 : Port to send the control message, Port 2: Port to send the data message (in current implementation Port 2 is not in use, all the message are send on control port. It can be used in future to transfer message on separate ports.) All the parameters must be filled.

{

 { "ID":"0", "IP" : "127.0.0.1" , "Port1" : "8081", "Port2" : "8091" }

}

##### TestConfig.json 
TestConfig.json this file contains the configuration parameters for the test cluster. It is similar to server.json file.

###API

Raft package provides a NewRaft method which take myid ( of the local raft Instance),  path ( path of the configuration files ),   server  (server Instance , user must set logger of server instance before passing), kvStorePath (path where backend key value store is present where final data you be stored), isRestart  (specify whether it is a restart or fresh start of raft instance) and return a pointer to the new raft instance.

{

	func NewRaft(myid int, path string, logLevel int, server *cluster.Server, kvStorePath string, isRestart bool) (*consensus, bool, error) 

}

A Raft instance provide following interface by which a raft layer's client can communicate to raft instance. This also have some method in interface which are being used for Debugging purpose.

type Raft interface {

    Term()     int   // return the current term 

    IsLeader() bool  // return whether the current raft instance is Leader 

    Pid() int		// return the Pid id current Raft instance
    
    Outbox() chan <\- interface{} // Return a out channel on which client can send commands

    Inbox() <\- chan *LogItem //Return In channel from where client can read the output for raft 

    Block(duration time.Duration) bool // Block the raft instance for specific duration

    Shutdown() // Shutdown the raft instance 

    LastLogIndexAndTerm() (int64,int64) // Return the lastLogTerm and Index (For Debug)
  
    PrintLog()                           // Print the Log in logging File (For Debug)
	
}
 

After starting the raft instances. Client can send a request in form of Command struct (shown below this paragraph). Current implementation support three commands : Put, Get, Del. Final client of Key-Value store will get Term, IsLeader, Get, Put and Del functionalities.

type Command struct{

	CmdId int64      // unique command Id send by client

	Cmd int			 // Command Type {Get, Put, Del ,IsLeader ,Term , LastLogTermAndIndex}

	Key []byte		 // Key in byte array 

	Value [] byte	 // Value in byte array	

}




Based on the type of command the raft layer provides the proper response in repose in LogItem struct. 

Each command contains one unique id (CmdId) which uniquely identify the each command and ensure each command is applied only once. This feature ensure the idempotent feature of the system. But system is implemented such that the if client do not want this feature he can pass the command with out the CmdId. 

The response from client would come in LogItem struct


type LogItem struct {

	Index int64 	// Index of Command

	Data  interface{}   // Returned Data

}


The error also send to using following Index value in reply message. Notice index value can not be negative in case of success.

{

	index = -1 (Not Leader) , Data will contain leaderId as int

	index = -2 (Error), Data will contain the error object

	index = -3 (Command Not supported)

	index = -4 (Contains term) , Data will contain Term as int64

	index = -5 (Last Index & Term pair), Data will contain IndexTerm struct (see API docs)

}

As system also log the Get command, it ensures the linearizable semantics. ( We should avoid logging of Get commond, this is drawbacl of system )

##Building##
The raft module depends on the cluster for underlying communication. Which internally  usage the Zeromq. Apart from this for logging and storage purpose it also uses the LevelDB.

1. To build the project you need to install the zmq4 golang binding. Binding are present at [Zeromq Go Binding](https://github.com/pebbe/zmq4)
2. Install the Download and install LevelDB present at [http://code.Google.com/p/leveldb/](http://code.google.com/p/leveldb/)
3. Install LevelDB go binding present at [https://github.com/jmhodges/levigo](https://github.com/jmhodges/levigo)
4. Get Raft from git hub. *"go get github.com/abhishekg16/raft"*
5. Go to the raft folder and the give command *"go install".*
6. Go to raft directory and give command *"go test -v*" to run the test case.
7. In order to run the benchmark give command *"go test -v -run=XXX -bench=."*
8. For more involved test Case testing folder have Testcases which kills the server on fly and restarts them (try to simulate real time environment).
	
	To run this test case use following command

1.  **mv raftTestSet1_test.go testing/**
2.  **mv testing/raft_test.go ./**
3.  Open raft_test.go and update to const present at top : Path : Path of the go's bin folder, currDir : Path of the root Dir of the raft  
4.  **cd testing/test_main/**
5.  **go install**
6.  come back to raft base directory
5.  **go test -v**


##TestCases##
Following Testcases are implemented to test the correctness of the system. Testing the system might take some time as some amount of delay has been added in testcases to simulate different test scenarios. See the raftTestSet1.test for details (what each test case does).

1. TestRaft_SingleLeaderInATerm
2. TestRaft_PutAndGetTest
3. TestRaft_MultipleCommandTestWithDelay  
4. TestRaft_RestartLeader
5. TestRaft_Idempotent
6. TestRaft_IntroducePartition
7. One test case to kill servers in fly and restart them again to simulate the real scenario.



##Example##
Here are some code sippet of code which shows how to instantiate raft object. For details see testing/test_main/raft_main.go file. 
testing/raft_test.go shows how a client can be implemented.


######testing/test_main/raft_main.go

{
	

	var s cluster.Server   // creating server
	var err error
	serverConfPath := path + "/conf/servers.json"
	s, err = cluster.New(pid, serverConfPath, nil, serverlogLevel)
	if err != nil {
		fmt.Println("Cound not instantiate server , Error : %v", err)
		return
	}
	
	// creating KV store
	var kvPath string
	kvPath = path + kvDir + "/" + kvName + strconv.Itoa(pid)
	// allocate log KV store
	if (isRestart == false) {
		err := db.DestroyDatabase(kvPath)
		if err != nil {
			log.Printf("Test : Cound not destroy previous KV the : %v ",kvPath )
			log.Printf("Test : Error %v ",err )
			s.Shutdown()
			return
		}
		conn := db.InitializeNewConnection()
		err = conn.OpenConnection(kvPath)
		if err != nil {
			log.Printf("Test : Cound not create connection : %v",kvPath )
			s.Shutdown()
			return
		} 
		conn.Close()
		if LOG_LEVEL > 0{	
			log.Println("Allocated new log data base")
		}
	}
	
	// create instance
	raftConfPath := path + "/conf"
	rObj, ok, err := raft.NewRaft(pid, raftConfPath, raftlogLevel, &s, kvPath ,isRestart)
	if ok == false {
			log.Println("Error Occured : Can in instantiate Raft Instances")
			s.Shutdown()
			return
	}
	if err != nil {
		log.Println(err)
		fmt.Println("errorr occured")
		s.Shutdown()
		return
	}

}


 	





	



 




