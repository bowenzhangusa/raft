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
	"log"
	"fmt"
)

// how often to send heartbeats
const HEARTBEAT_FREQUENCY = 100 * time.Millisecond

const EVENT_VOTES_GRANTED = 1
const EVENT_APPEND_ENTRIES_RECEIVED = 2
const EVENT_APPEND_ENTRIES_SEND_SUCCESS = 3

const STATUS_FOLLOWER = 0
const STATUS_CANDIDATE = 1
const STATUS_LEADER = 2

// This is passed from RPC handlers to "handleEvent"
// to keep business logic in one place
type Event struct {
	Type int // see constants above
	Term int // term from a peer
	Peer int // peer that initiated the event (RPC caller)
}

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
	Term    int // term when the entry is received by the leader, starts at 1
	Position int // position in the log
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
	status    int   // status of a raft. 0 means follower, 1 means candidate, 2 means leader
	nextIndex []int // for each server, index of the next log entry to send to that server
	// initialized to leader's last log index + 1
	matchIndex []int // for each server, index of highest log entry known to be replicated on that server
	// initialized to zero, increases monotonically

	electionTimer *time.Timer
	eventCh       chan Event

	// client request channel
	requestCh     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == STATUS_LEADER
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
	PeerIndex int // index of the raft instance in leader's

	// TODO: we may want to add info for the term of the conflicting entry for later optimization
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.becomeFollowerIfTermIsOlderOrEqual(args.Term, fmt.Sprintf("AppendEntries request from %d", args.LeaderId))

	if args.Term < rf.currentTerm { // This happens when an old failed leader just woke up
		reply.Success = false
	} else {
		if len(args.LogEntries) == 0 { // this is heartbeat
			rf.eventCh <- Event{Type: EVENT_APPEND_ENTRIES_RECEIVED, Peer: args.LeaderId}
			reply.Success = true
		} else {
			// check if we have log consistency
			if args.PrevLogIndex >= len(rf.logEntries) {
				reply.Success = false
			} else if args.PrevLogTerm != rf.logEntries[args.PrevLogIndex].Term {
				reply.Success = false
			} else {
				reply.Success = true
				// append leader's log to its own logs
				rf.logEntries = append(rf.logEntries, args.LogEntries...)
				if args.LeaderCommitIndex > rf.commitIndex {
					rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logEntries) - 1)
				}
			}
		}
	}

	reply.Term = rf.currentTerm
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
	rf.becomeFollowerIfTermIsOlder(args.Term, fmt.Sprintf("RequestVote request from %d", args.CandidateId))
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.votedFor == 0 { // first check to grant vote is that raft has yet to vote in the term
		if rf.currentTerm < args.Term { // If a new term starts, grant the vote
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else if rf.currentTerm == args.Term { // if in the same term, whoever has longer log is more up-to-date
			if len(rf.logEntries) <= args.LastLogIndex+1 {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			}
		}
	}

	rf.DPrintf(
		"received vote request from %d, granted: %t",
		args.CandidateId, reply.VoteGranted)
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

// Send RequestVote to all peers and collect results
func (rf *Raft) sendRequestVoteToAllPeers() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.currentTerm,
		LastLogIndex: 0, //TODO
	}
	rf.mu.Unlock()

	// to send response structure and "ok" flag in a channel,
	// we need to wrap it in a structure
	type ResponseMsg struct {
		RequestVoteReply
		IsOk bool
	}
	responseChan := make(chan ResponseMsg)
	rf.DPrintf("sending RequestVote")

	// send requests concurrently
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peerIndex int) {
			resp := RequestVoteReply{}
			ok := rf.sendRequestVote(peerIndex, &args, &resp)
			rf.DPrintf("received RequestVote response from %d, ok: %t, granted: %t", peerIndex, ok, resp.VoteGranted)
			responseChan <- ResponseMsg{
				resp,
				ok,
			}
		}(i)
	}

	grantedVoteCount := 1 // initial vote is a vote for self

	// collect responses
	for resp := range responseChan {
		isGranted := resp.IsOk && resp.VoteGranted

		if resp.IsOk {
			//rf.BecomeFollowerIfTermIsOlder(resp.Term, false, "RequestVotes response")
			if isGranted {
				grantedVoteCount++
				// if enough responses received, send the result on a channel
				// - don't need to wait for other responses
				if grantedVoteCount == rf.getMajoritySize() {
					rf.eventCh <- Event{Type: EVENT_VOTES_GRANTED}
					return
				}
			} else {
				rf.becomeFollowerIfTermIsOlder(resp.Term, "RequestVotes response")
			}
		}


	}
}

// Send AppendEntries to given peer
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Send AppendEntries to all peers and collect results
func (rf *Raft) sendAppendEntriesToAllPeersForHeartbeats() {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LeaderCommitIndex: rf.commitIndex,
		LogEntries:        []Log{},
		// TODO fill these correctly
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}
	rf.mu.Unlock()

	// to send response structure and "ok" flag in a channel,
	// we need to wrap it in a structure
	type ResponseMsg struct {
		AppendEntriesReply
		IsOk bool
		Peer int
		// for debugging purposes - to see how delayed the response was
		DateSent time.Time
	}
	responseChan := make(chan ResponseMsg)
	rf.DPrintf("sending AppendEntries")

	// send requests concurrently
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peerIndex int) {
			resp := AppendEntriesReply{}
			dateSent := time.Now()
			ok := rf.sendAppendEntries(peerIndex, &args, &resp)
			responseChan <- ResponseMsg{
				resp,
				ok,
				peerIndex,
				dateSent,
			}
		}(i)
	}

	successCount := 1 // count ourselves
	failCount := 0

	// collect responses
	for resp := range responseChan {
		rf.DPrintf("received AppendEntries response from %d, ok: %t, success: %t, sent at: %s", resp.Peer, resp.IsOk, resp.Success, resp.DateSent.Format(time.StampMicro))

		// no network/host failure AND host agreed to append
		isOk := resp.IsOk && resp.Success

		if resp.IsOk {
			// this happens when we just woke up as a previous leader
			rf.becomeFollowerIfTermIsOlder(resp.Term,  "AppendEntries response")
		}

		// if enough responses received, send the result on a channel
		// - don't need to wait for others
		if isOk {
			successCount++
			if successCount == rf.getMajoritySize() {
				rf.eventCh <- Event{Type: EVENT_APPEND_ENTRIES_SEND_SUCCESS}
			}
		} else {
			failCount++
		}
	}
}

// Wrapper for debug print function,
// that prints current host id and term automatically
func (rf *Raft) DPrintf(format string, a ...interface{}) {
	if Debug == 1 {
		args := make([]interface{}, 0, 3+len(a))
		args = append(append(args, rf.me, rf.status, rf.currentTerm), a...)
		DPrintf("[i%d s%d t%d] "+format, args...)
	}
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
	if rf.status != STATUS_LEADER {
		return -1, -1, false
	}

	rf.mu.Lock()
	newLog := Log{Command: command, Term:rf.currentTerm, Position:len(rf.logEntries)}
	rf.logEntries = append(rf.logEntries, newLog)
	newLength := len(rf.logEntries)
	rf.mu.Unlock()
	go rf.sendAppendEntriesToAllPeersForLogEntries(newLength-1)
	return newLength-1, rf.currentTerm, true
}

// Send AppendEntries to all peers and collect results
func (rf *Raft) sendAppendEntriesToAllPeersForLogEntries(lastLogIndexFromLeader int) {
	// to send response structure and "ok" flag in a channel,
	// we need to wrap it in a structure
	type ResponseMsg struct {
		AppendEntriesReply
		IsOk bool
		Peer int
		// for debugging purposes - to see how delayed the response was
		DateSent time.Time
	}
	responseChan := make(chan ResponseMsg)
	rf.DPrintf("sending AppendEntries")

	// Send requests concurrently
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peerIndex int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex: lastLogIndexFromLeader - 1,
				PrevLogTerm:  rf.logEntries[lastLogIndexFromLeader-1].Term,
				LogEntries:        rf.logEntries[rf.nextIndex[peerIndex] : lastLogIndexFromLeader + 1],
				LeaderCommitIndex: rf.commitIndex,
			}
			rf.mu.Unlock()

			resp := AppendEntriesReply{
				PeerIndex:peerIndex,
			}
			dateSent := time.Now()
			ok := rf.sendAppendEntries(peerIndex, &args, &resp)
			responseChan <- ResponseMsg{
				resp,
				ok,
				peerIndex,
				dateSent,
			}
		} (i)
	}


	successCount := 1 // count ourselves
	failCount := 0

	// collect responses
	for resp := range responseChan {
		rf.DPrintf("received AppendEntries response from %d, ok: %t, success: %t, sent at: %s", resp.Peer, resp.IsOk, resp.Success, resp.DateSent.Format(time.StampMicro))

		// no network/host failure AND host agreed to append
		isOk := resp.IsOk && resp.Success

		if resp.IsOk {
			// this happens when we just woke up as a previous leader
			rf.becomeFollowerIfTermIsOlder(resp.Term,  "AppendEntries response")
		}

		// if enough responses received, send the result on a channel
		// - don't need to wait for others
		if isOk {
			successCount++

			// Update the match index and next index for this particular follower
			rf.matchIndex[resp.PeerIndex] = rf.nextIndex[resp.PeerIndex]
			rf.nextIndex[resp.PeerIndex] = rf.nextIndex[resp.PeerIndex] + 1

			if successCount == rf.getMajoritySize() {
				rf.eventCh <- Event{Type: EVENT_APPEND_ENTRIES_SEND_SUCCESS}
			}
		} else {
			failCount++

			// TODO: We need to handle failure of log consistency here
		}
	}
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

// Turns current host into leader
func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != STATUS_LEADER {
		rf.status = STATUS_LEADER
		rf.votedFor = 0
		rf.DPrintf("became a leader")
	}

	if !rf.electionTimer.Stop() {
		<- rf.electionTimer.C
	}

	// Initialize all nextIndex values to the next Index the leader will send to followers
	rf.nextIndex = make([]int, len(rf.peers))
	for index,_ := range rf.peers {
		rf.nextIndex[index] = len(rf.logEntries)
	}

	// send heartbeat immediately without waiting for a ticker
	// to make sure other peers will not timeout.
	go rf.sendAppendEntriesToAllPeersForHeartbeats()
}

// Turns current host into candidate
func (rf *Raft) BecomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.status = STATUS_CANDIDATE
	rf.DPrintf("old term number is %d", rf.currentTerm)
	rf.currentTerm++
	rf.votedFor = rf.me
}

// Turns current host into follower during election because either we discovered the current leader or a new turn
func (rf *Raft) becomeFollowerIfTermIsOlder(term int, comment string) {
	// TODO: we will see if we need a lock here
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check if we have a new term
	if (rf.currentTerm < term) {
		//rf.becomeFollower()
		if rf.status != STATUS_FOLLOWER {
			rf.status = STATUS_FOLLOWER
			rf.electionTimer.Reset(getElectionTimeout())
		}
		oldTerm := rf.currentTerm
		rf.currentTerm = term
		rf.votedFor = 0
		rf.DPrintf(
			"[%s] ELECTION term updated, old: %d. Host is now a follower",
			comment, oldTerm)
	}
}
// Turns current host into follower and updates its term, if given term is newer.
// Comment is used only for debug.
func (rf *Raft) becomeFollowerIfTermIsOlderOrEqual(term int, comment string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (rf.currentTerm <= term) { // a new leader sends a heartbeat
		rf.status = STATUS_FOLLOWER
		oldTerm := rf.currentTerm
		rf.currentTerm = term
		rf.votedFor = 0
		rf.electionTimer.Reset(getElectionTimeout())
		rf.DPrintf(

			"[%s]  NOT ELECTION term updated, old: %d. Host is now a follower",
			comment, oldTerm)
	}
}

// Returns the number of hosts that forms a majority
func (rf *Raft) getMajoritySize() int {
	return len(rf.peers)/2 + 1
}

// Listens for events and timers
func (rf *Raft) listen() {
	heartbeatTicker := time.NewTicker(HEARTBEAT_FREQUENCY)
	rf.DPrintf("started")

	for {
		select {
		case event := <-rf.eventCh:
			rf.handleEvent(event)
			break;
		case <-rf.electionTimer.C:
			// time to initiate an election
			rf.DPrintf("election timeout")
			rf.BecomeCandidate()
			go rf.sendRequestVoteToAllPeers()
			rf.electionTimer.Reset(getElectionTimeout())
			break;
		case <-heartbeatTicker.C:
			// time to send a heartbeat
			if rf.status == STATUS_LEADER {
				go rf.sendAppendEntriesToAllPeersForHeartbeats()
			}
			break;
		case <-rf.requestCh:
			// time to process request from tester

		}
	}
}

// Handles events, such as results of RPCs
func (rf *Raft) handleEvent(event Event) {
	switch (event.Type) {
	case EVENT_VOTES_GRANTED:
		if rf.status == STATUS_CANDIDATE {
			rf.BecomeLeader()
		} else {
			// this might happen when votes from some older term are received,
			// but this host is not a candidate any more, so we ignore it
			rf.DPrintf("got votes, but host is not a candidate")
		}
		break
	case EVENT_APPEND_ENTRIES_SEND_SUCCESS:
		// TODO: commit entries
		rf.DPrintf("got AppendEntries response from majority")
		break
	case EVENT_APPEND_ENTRIES_RECEIVED:
		// TODO: add entries to log
		rf.electionTimer.Reset(getElectionTimeout())
		rf.DPrintf("AppendEntries received from %d", event.Peer)
		break;
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
	log.SetFlags(log.Lmicroseconds)
	rf.peers = peers
	rf.me = me
	rf.status = STATUS_FOLLOWER
	rf.electionTimer = time.NewTimer(getElectionTimeout())
	// event channel is used to consolidate business logic in a single function (handleEvent).
	// it is not meant to process events asynchronously, so its buffer size is 1.
	rf.eventCh = make(chan Event, 1)

	rf.requestCh = applyCh

	go rf.listen()

	return rf
}
