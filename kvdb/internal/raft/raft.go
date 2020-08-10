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

	"kvdb/internal/labgob"
	"kvdb/internal/labrpc"
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
		Snapshot     []byte
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

		state           serverState // follower, candidate or leader
		voteCount       int
		lastAction      time.Time
		electionTimeout time.Duration

		applyCond *sync.Cond
		applyCh   chan ApplyMsg

		lastIncludedIndex int // Last included index in the latest snapshot
		lastIncludedTerm  int // Term of the last included index
	}

	// LogEntry represents the content in each log entry.
	LogEntry struct {
		Term    int
		Command interface{}
	}
)

// GetState returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (term int, isleader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

// Caller ensures lock acquired.
func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// Save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// Caller ensures lock acquired.
func (rf *Raft) persist() {
	data := rf.encodeRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	state := rf.encodeRaftState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("readPersist error: server %d", rf.me)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		isLeader = false
		return
	}
	index = rf.lastLogicalLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()
	go rf.replicate()
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
	rf.applyCond.Signal()
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
		me:        me,
		peers:     peers,
		persister: persister,
		state:     follower,
		votedFor:  -1,
	}
	rf.log = append(rf.log, LogEntry{})
	rf.resetElectionTimer()
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.readPersist(persister.ReadRaftState()) // initialize from state persisted before a crash

	go rf.periodicElections()
	go rf.periodicHeartbeats()
	go rf.applyLogLoop()

	return rf
}

// Caller ensure lock acquired.
func (rf *Raft) startElection() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.resetElectionTimer()
	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.votedFor,
		LastLogIndex: rf.lastLogicalLogIndex(),
		LastLogTerm:  rf.log[rf.toPhysicalIndex(rf.lastLogicalLogIndex())].Term,
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
		if rf.state != leader {
			if time.Now().Sub(rf.lastAction) >= rf.electionTimeout {
				rf.startElection()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// Caller acquried lock.
func (rf *Raft) installSnapshot(i int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(i, &args, &reply)
	rf.mu.Lock()
	if ok {
		rf.handleInstallSnapshotReply(i, &args, &reply)
	}
}

func (rf *Raft) appendEntries(i int) (retry bool) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.nextIndex[i] - 1,
	}
	args.PrevLogTerm = rf.log[rf.toPhysicalIndex(args.PrevLogIndex)].Term
	if rf.lastLogicalLogIndex() >= rf.nextIndex[i] {
		args.Entries = append(args.Entries, rf.log[rf.toPhysicalIndex(rf.nextIndex[i]):]...)
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, &args, &reply)
	rf.mu.Lock()
	if ok {
		if retry := rf.handleAppendEntriesReply(i, &args, &reply); !retry {
			return false
		}
	}
	return true
}

// Caller ensure lock acquried.
func (rf *Raft) replicate() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != leader {
				return
			}
			if rf.lastIncludedIndex >= rf.nextIndex[i] {
				rf.installSnapshot(i)
				return
			}
			rf.appendEntries(i)
		}(i)
	}
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (5.3, 5.4).
func (rf *Raft) updateCommitIndexUnlocked() {
	for N := rf.lastLogicalLogIndex(); N > rf.commitIndex; N-- {
		if rf.log[rf.toPhysicalIndex(N)].Term != rf.currentTerm {
			continue
		}
		count := 1
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			if rf.matchIndex[server] >= N {
				count++
				if count > len(rf.peers)/2 {
					rf.commitIndex = N
					rf.applyCond.Signal()
					return
				}
			}
		}
	}
}

// Long running loop for leader to send AppendEntries RPC
func (rf *Raft) periodicHeartbeats() {
	count := 0
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == leader {
			count = count % 10
			if count == 0 {
				rf.replicate()
			}
			count++
		} else {
			count = 0
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// Long running loop for apply committed log entries
//
// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (5.3)
func (rf *Raft) applyLogLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}
		rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
		rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.toPhysicalIndex(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
		} else {
			rf.mu.Unlock()
		}
	}
}

// Caller ensure lock acquired.
func (rf *Raft) lastLogicalLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// Caller ensure lock acquired.
func (rf *Raft) toPhysicalIndex(logicalIndex int) int {
	return logicalIndex - rf.lastIncludedIndex
}

// Caller ensure lock acquired.
func (rf *Raft) switchToFollower(term int) {
	if rf.state != follower {
		rf.resetElectionTimer()
	}
	rf.state = follower
	rf.votedFor = -1
	rf.currentTerm = term
	rf.persist()
}

// Caller ensure lock acquired.
func (rf *Raft) resetElectionTimer() {
	rf.lastAction = time.Now()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionTimeout = time.Duration(r.Intn(300)+300) * time.Millisecond
}

func (rf *Raft) TakeSnapshot(lastIncludedIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedTerm = rf.log[rf.toPhysicalIndex(lastIncludedIndex)].Term
	rf.log = rf.log[rf.toPhysicalIndex(lastIncludedIndex):]
	rf.lastIncludedIndex = lastIncludedIndex
	rf.persistWithSnapshot(snapshot)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != leader {
				return
			}
			rf.installSnapshot(i)
		}(i)
	}
}
