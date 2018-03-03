package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"../labrpc"
	"time"
	"math/rand"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// this is our representation of Log entry which contains the command
// and  a term when the entry is received by the leader
//
type Log struct {
	Command interface{}
	term    int // term when the entry is received by the leader, starts at 1
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	currentTerm int //This is the term number starting at 1
	votedFor    int //CandidateId that this server voted for in this term
	logEntries  []Log

	// The following variables are volatile states on all servers
	// Both of the following indices increase monotonically and cannot decrease or go back
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machines

	// The following are leader related properties
	isLeader  bool
	nextIndex []int // for each server, index of the next log entry to send to that server
	// initialzed to leader's lasst log index + 1
	matchIndex []int // for each server, index of highest log entry known to be replicated on that server
	// initialized to zero, increases monotonically

	appendEntriesChannel chan *AppendEntriesArgs
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	return term, isleader
}

// example AppendEntriesRPC arguments structure
type AppendEntriesArgs struct {
	Term              int   // term number
	LeaderId          int   // id of the leader
	PrevLogIndex      int   // index of the log immediately preceding new ones
	PrevLogTerm       int   // term of prevLogIndex entry
	LogEntries        []Log //log entries to store. For heartbeat, this is empty. May send more than one for efficiency
	LeaderCommitIndex int
}

// example AppendEntriesRPC reply structure
type AppendEntriesReply struct {
	Term    int  // term number
	Success bool //true if follower contains log entry matching PrevLogIndex and PrevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.appendEntriesChannel <- args

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // this is the term number of the election
	CandidateId  int // id of candidate requesting the vote
	LastLogIndex int // index of the candidate's last log entry
	LastLogTerm  int // term number of the candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // term number of the election
	VoteGranted bool // If the vote is granted
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	DPrintf("Request vote received from %d", args.CandidateId)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) listen() {
	for {
		select {
		case args := <-rf.appendEntriesChannel:
			DPrintf("AppendEntries received: %+v", args)

		case <-time.After(time.Duration(700+rand.Intn(300)) * time.Millisecond):
			DPrintf("Timeout, will request votes")
			rf.becomeCandidate()
		}
	}
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.votedFor = rf.me
	var lastLogTerm int

	if len(rf.logEntries) > 0 {
		lastLogTerm = rf.logEntries[len(rf.logEntries)-1].term
	} else {
		lastLogTerm = 0
	}

	args := RequestVoteArgs{
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: len(rf.logEntries) - 1,
	}
	responses := make([]RequestVoteReply, len(rf.peers))

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		DPrintf("Sending vote request to %d", i)
		rf.sendRequestVote(i, &args, &responses[i])
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.appendEntriesChannel = make(chan *AppendEntriesArgs, 1)

	go rf.listen()
	// Your initialization code here (3A, 3B).

	return rf
}
