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

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raft/pkg/labgob"
	"raft/pkg/labrpc"
)

const (
	follower serverState = iota + 1
	candidate
	leader
)

type (
	serverState int

	// ApplyMsg ...
	//
	// as each Raft peer becomes aware that successive log entries are
	// committed, the peer should send an ApplyMsg to the service (or
	// tester) on the same server, via the applyCh passed to Make(). set
	// CommandValid to true to indicate that the ApplyMsg contains a newly
	// committed log entry.
	//
	// in Lab 3 you'll want to send other kinds of messages (e.g.,
	// snapshots) on the applyCh; at that point you can add fields to
	// ApplyMsg, but set CommandValid to false for these other uses.
	//
	ApplyMsg struct {
		CommandValid bool
		Command      interface{}
		CommandIndex int
	}

	// Raft implements a single Raft peer.
	Raft struct {
		mu        sync.Mutex          // Lock to protect shared access to this peer's state
		peers     []*labrpc.ClientEnd // RPC end points of all peers
		persister *Persister          // Object to hold this peer's persisted state
		me        int                 // this peer's index into peers[]
		dead      int32               // set by Kill()

		// Persistent state on all servers: (Updated on stable storage before responding to RPCs)
		currentTerm int
		votedFor    int
		log         []LogEntry

		// Volatile state on all servers:
		commitIndex int
		lastApplied int

		// Volatile state on leaders: (Reinitialized after election)
		nextIndex  []int
		matchIndex []int

		state serverState // follower, candidate or leader

		leaderCond   *sync.Cond
		followerCond *sync.Cond
		applyLogCond *sync.Cond

		voteCount int

		lastAction      time.Time
		electionTimeout time.Duration
	}

	// LogEntry represents the content in each log entry.
	LogEntry struct {
		Term    int
		Command interface{}
	}

	// RequestVoteArgs is RequestVote RPC arguments structure.
	// field names must start with capital letters!
	RequestVoteArgs struct {
		// Your data here (2A, 2B).
		Term         int // candidate’s term
		CandidateID  int // candidate requesting vote
		LastLogIndex int // index of candidate’s last log entry (§5.4)
		LastLogTerm  int // term of candidate’s last log entry (§5.4)
	}

	// RequestVoteReply is RequestVote RPC reply structure.
	// field names must start with capital letters!
	RequestVoteReply struct {
		// Your data here (2A).
		Term        int  // currentTerm, for candidate to update itself
		VoteGranted bool // true means candidate received vote
	}

	// AppendEntriesArgs is AppendEntries RPC arguments structure.
	AppendEntriesArgs struct {
		Term         int        // leader’s term
		LeaderID     int        // so follower can redirect clients
		PrevLogIndex int        // index of log entry immediately preceding new ones
		PrevLogTerm  int        // term of prevLogIndex entry
		Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
		LeaderCommit int        // leader’s commitIndex
	}

	// AppendEntriesReply is AppendEntries RPC reply structure.
	AppendEntriesReply struct {
		Term    int  // currentTerm, for leader to update itself
		Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

		ConflictIndex int
		ConflictTerm  int
	}
)

// GetState returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (term int, isleader bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

// Save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// Caller ensures lock acquired.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		//   error...
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
	}
}

// RequestVote RPC handler.
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state == leader {
			rf.followerCond.Signal()
		}
		rf.state = follower
		rf.votedFor = -1
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm || rf.state != follower {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.isCandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.lastAction = time.Now()
		rf.electionTimeout = time.Duration(r1.Intn(300)+300) * time.Millisecond
	}

	rf.persist()
}

// Caller ensure lock acquired.
func (rf *Raft) isCandidateLogUpToDate(candidateTerm int, candidateIdx int) bool {
	idx := rf.lastLogIndex()
	term := rf.log[idx].Term

	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	if term != candidateTerm {
		return candidateTerm >= term
	}

	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	return candidateIdx >= idx
}

// AppendEntries RPC handler.
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state == leader {
			rf.followerCond.Signal()
		}
		rf.state = follower
		rf.votedFor = args.LeaderID
	}

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1

	if rf.state != follower || args.Term < rf.currentTerm {
		return
	}

	rf.lastAction = time.Now()
	rf.electionTimeout = time.Duration(r1.Intn(300)+300) * time.Millisecond

	defer rf.persist()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)

	// If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = len(rf.log)
		return
	}

	// If a follower does have prevLogIndex in its log, but the term does not match, it should return conflictTerm = log[prevLogIndex].Term,
	// and then search its log for the first index whose entry has term equal to conflictTerm.
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		for i := 1; i < len(rf.log); i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}

		if reply.ConflictIndex == 0 {
			DPrintf("!!!ERROR!!!")
		}

		return
	}

	reply.Success = true

	if len(args.Entries) > 0 {
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		i := args.PrevLogIndex + 1
		j := 0
		truncate := false
		for ; i < len(rf.log); i, j = i+1, j+1 {
			if j >= len(args.Entries) {
				break
			}

			if rf.log[i].Term != args.Entries[j].Term || rf.log[i].Command != args.Entries[j].Command {
				truncate = true
				break
			}
		}

		if truncate {
			rf.log = rf.log[:i]
		}

		// 4. Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries[j:]...)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIdx := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = Min(args.LeaderCommit, lastNewEntryIdx)
		rf.applyLogCond.Signal()
	}
}

// send a RequestVote RPC to a server.
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start ...
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		isLeader = false
		return
	}

	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.replicate()
	}()

	return
}

// Kill ...
//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.leaderCond.Broadcast()
	rf.followerCond.Signal()
	rf.applyLogCond.Signal()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make a Raft server.
// Args:
//   peers:     The ports of all the Raft servers (including this one) are in peers[]. All the servers' peers[] arrays have the same order.
//   me:        This server's port is peers[me].
//   persister: A place for this server to save its persistent state, and also initially holds the most recent saved state, if any.
//   applyCh:   A channel on which the tester or service expects Raft to send ApplyMsg messages.
//
// Make() must return quickly, so it should start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		me:              me,
		peers:           peers,
		persister:       persister,
		state:           follower,
		votedFor:        -1,
		lastAction:      time.Now(),
		electionTimeout: time.Duration(r1.Intn(300)+300) * time.Millisecond,
	}
	rf.log = append(rf.log, LogEntry{})

	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.followerCond = sync.NewCond(&rf.mu)
	rf.applyLogCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.periodicElections()
	go rf.periodicHeartbeats()
	go rf.periodicUpdateCommitIndex()
	go rf.applyLog(applyCh)

	return rf
}

func (rf *Raft) handleVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. No need to handle RequestVote RPC reply if have switched state
	// 2. Old RPC reply, currentTerm has been updated, drop the reply and return
	if rf.state != candidate || args.Term != rf.currentTerm {
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = follower

		rf.persist()

		rf.lastAction = time.Now()
		rf.electionTimeout = time.Duration(r1.Intn(300)+300) * time.Millisecond

		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > (len(rf.peers) / 2) {
			rf.state = leader
			rf.initIndex()
		}
	}
}

// Reinitialize nextIndex[] and matchIndex[] after election
func (rf *Raft) initIndex() {
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}

	rf.leaderCond.Broadcast()
}

// Caller ensure lock acquired.
func (rf *Raft) startElection(now time.Time) {
	rf.state = candidate

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.electionTimeout = time.Duration(r1.Intn(300)+300) * time.Millisecond
	rf.lastAction = now

	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.votedFor,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		server := i

		go func(i int, args RequestVoteArgs) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				rf.handleVoteReply(&args, &reply)
			}
		}(server, args)
	}
}

// Long running loop to perform election
func (rf *Raft) periodicElections() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.state == leader {
			rf.followerCond.Wait()

			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		now := time.Now()
		if now.Sub(rf.lastAction) >= rf.electionTimeout {
			rf.startElection(now)
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader || args.Term != rf.currentTerm {
		return
	}

	if !reply.Success {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			if rf.state == leader {
				rf.followerCond.Signal()
			}
			rf.state = follower

			rf.persist()

			rf.lastAction = time.Now()
			rf.electionTimeout = time.Duration(r1.Intn(300)+300) * time.Millisecond

			return
		}

		// Follower didn't set ConflictIndex, don't perform optimization
		if reply.ConflictIndex == 0 {
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
			return
		}

		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
			return
		}

		for i := rf.lastLogIndex(); i >= 1; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				rf.nextIndex[server] = i + 1
				return
			}
		}

		rf.nextIndex[server] = reply.ConflictIndex
	} else {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}
}

// Caller ensure lock acquried.
func (rf *Raft) replicate() {
	for i := range rf.peers {

		if i != rf.me {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.nextIndex[i] - 1,
			}
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			if rf.lastLogIndex() >= rf.nextIndex[i] {
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]:]...)
			}

			go func(i int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(i, &args, &reply) {
					rf.handleAppendEntriesReply(i, &args, &reply)
				}
			}(i, args)
		}

	}
}

// Long running loop for leader to update commitIndex
func (rf *Raft) periodicUpdateCommitIndex() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.state != leader {
			rf.leaderCond.Wait()

			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
		for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {

			if rf.log[N].Term > rf.currentTerm {
				continue
			}

			if rf.log[N].Term < rf.currentTerm {
				break
			}

			count := 1
			done := false
			for server := range rf.peers {
				if rf.matchIndex[server] >= N {
					count++
					if count > len(rf.peers)/2 {
						rf.commitIndex = N
						rf.applyLogCond.Signal()
						done = true
						break
					}
				}
			}

			if done {
				break
			}

		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// Long running loop for leader to send AppendEntries RPC
func (rf *Raft) periodicHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.state != leader {
			rf.leaderCond.Wait()

			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		rf.replicate()
		rf.mu.Unlock()

		time.Sleep(time.Duration(120) * time.Millisecond)
	}
}

// Long running loop for apply committed log entries
func (rf *Raft) applyLog(applyCh chan ApplyMsg) {
	for !rf.killed() {
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
		rf.mu.Lock()

		for rf.commitIndex <= rf.lastApplied {
			rf.applyLogCond.Wait()

			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}

		rf.mu.Unlock()

		applyCh <- msg
	}
}

// Caller ensure log acquired.
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

var s1 rand.Source
var r1 *rand.Rand

func init() {
	s1 = rand.NewSource(time.Now().UnixNano())
	r1 = rand.New(s1)
}
