#Raft (Consensus Module)#
This project provides an implementation of Raft consensus algorithm. The consensus algorithm guarantee that there would be only one leader a time in the system for a term. The raft also provides the well defined interface by using which you can get information about the  leader and term. 
	Raft uses the cluster implementation as a underlying communication channel which allows different raft instances to communicate over the network. It also uses the LevelDB to store the log messages. 

####Features of Implementation ####
1. Provides easily configurable files 
2. File logging facilities
3. Provide two different port for the message and control message (But not used in current Implementation)


#### Limitation ####
1. The storage is linked with the raplication layer which is a bad design. Need to be fixed.
2. More Testcases need to be added.

##Usages##
Raft provides a set of configuration files which can be easily configured to control the system behaviour. All the configuration files are present in the conf folder

##### raft.json 
raft.json have all the configuration parameters related to the raft instance.

1. myid : Id of the raft instance 
2. Etimeout : Election timeout
2. hfre : Time interval between the heartbeat signals
3. dataDir : directory where all the persistent data related to raft is stored
4. logDir : directory where all the log message are dumped


##### servers.json
servers.json file have all the configuration parameter related to the cluster instance. It contains the ID the socket address of all the peer instances.

##### TestConfig.json 
TestConfig.json this file contains the configuration parameters for the test cluster

###API

Raft package provides a New method which take myid ( of the local server),  path ( path of the configuration files ),   server  (serverInstance (set server logger before sending),  isRestart  (specify whether it is a restart or fresh start of raft isntance) and return a pointer to the new raft instance.

{

	func NewRaft(myid int, path string, logLevel int, server *cluster.Server, isRestart bool) (*consensus, bool, error) 

}

A Raft instance provide following interface by which client can communicate to raft instance. Apart from this also have some method in interface which are being used for Debugging purpose.

type Raft interface {

    Term()     int   // return the current term 

    IsLeader() bool  // return whether the current raft instance is Leader 

    Pid() int		// return the Pid id current Raft instance
    
    Outbox() chan <\- interface{} // Return a out channel on which client can send commands

    Inbox() <\- chan *LogItem //Return In channel from where client can read the output for raft 

    Block(duration time.Duration) bool // Block the raft instance for specific duration

    Shutdown() // Shutdown the raft instance 

   // DEBUG
   LastLogIndexAndTerm() (int64,int64) // Return the lastLogTerm and Index
	
}
 


##Building##
The raft module depends on the cluster for underlying communication. Which internally  usage the Zeromq. Apart from this for logging purpose it also uses the LevelDB.

1. To build the project you need to install the zmq4 golang binding. Binding are present at [Zeromq Go Binding](https://github.com/pebbe/zmq4)
2. Install the Download and install LevelDB present at [http://code.google.com/p/leveldb/](http://code.google.com/p/leveldb/)
3. Install LevelDB gobinding present at [https://github.com/jmhodges/levigo](https://github.com/jmhodges/levigo)
4. Get Raft from git hub. "go get github.com/abhishekg16/raft"
5. Go to the raft folder and the give command "go install".
6. Go to raft directory and give command "go test"


##Example##
Here is an example which instantiate a raft instance and ask for the leader. This main program take the pid of the raft instance as command line argument. So to start this method start following command

$go run raft\_main.go -id 0

######raft\_main.go

func main() {

	var f int
	flag.IntVar(&f, "id", -1, "enter pid")
	flag.Parse()
	if f == -1 {
		fmt.Println("Invalid Arguments")
		os.Exit(0)
	}
	// create instance
	rObj, _, err := raft.NewRaft(f, "./conf", 0)
	if err != nil {
		log.Println(err)
		fmt.Println("errorr occured")
		os.Exit(1)
	}
	fmt.Println(rObj.IsTerm()) 

}


 	





	



 




