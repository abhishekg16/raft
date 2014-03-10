#Raft (Consensus Module)#
This project provides an implementation of Raft consensus algorithm. The consensus algorithm guarantee that there would be only one leader a time in the system for a term. The raft also provides the well defined interface by using which you can get information about the  leader and term. 
	Raft uses the cluster implementation as a underlying communication channel which allows different raft instances to communicate over the network

####Features of Implementation ####
1. Provides easily configurable files 
2. File logging facilities
3. Provide two different port for the message and control message 

#### Limitation ####
1. Logging of raft message is not handled


##Usages##
Raft provides a set of configuration files which can be easily configured to control the system behaviour. All the configuration files are present in the conf folder

##### raft.json 
raft .json have all the configuration parameters related to the raft instance.

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

Raft package provide a New method which take myid ( of the local server),  path ( path of the configuration files ),  mode ( specify the mode of start, 0=> normal mode, 1=> debug mode). and return a pointer to the new raft instance.

{

	func NewRaft(myid int, path string, mode int) (*consensus, bool, error) 

}

A Raft instance provide following interface by which client can communicate to raft instance.

type Raft interface {

    Term()     int   // return the current term 

    IsLeader() bool  // return whether the current raft instance is Leader 

    Pid() int		// return the Pid id current Raft instance
    
    // Only available in DEBUG mode
    Block(duration time.Duration) bool // Block the raft instance for specific duration

    Resume() bool // Resume the blocked raft instance

    Shutdown() // Shutdown the raft instance 

}
 


##Building##
The raft module depends on the cluster for underlying communication. Which internally  usage the Zeromq. 

1. To build the project you need to install the zmq4 golang binding. Binding are present at https://github.com/pebbe/zmq4
2. Get Raft from git hub. "go git github.com/abhishekg16/raft"
3. Go to the raft/testing/test\_main directory. and use command "go install".
   This will install the binary in the $GOPATH/bin folder
4. Go to raft directory and give command "go test"


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


 	





	



 




