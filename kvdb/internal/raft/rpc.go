package raft

type (
	// RequestVoteArgs is RequestVote RPC arguments structure.
	// Field names must start with capital letters!
	RequestVoteArgs struct {
		Term         int // candidate’s term
		CandidateID  int // candidate requesting vote
		LastLogIndex int // index of candidate’s last log entry (5.4)
		LastLogTerm  int // term of candidate’s last log entry (5.4)
	}

	// RequestVoteReply is RequestVote RPC reply structure.
	// Field names must start with capital letters!
	RequestVoteReply struct {
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

	InstallSnapshotArgs struct {
		Term              int
		LeaderID          int
		LastIncludedIndex int
		LastIncludedTerm  int
		Data              []byte
	}

	InstallSnapshotReply struct {
		Term int
	}
)

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
	if args.Term > rf.currentTerm {
		rf.switchToFollower(args.Term)
	}
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm || rf.state != follower {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (5.2, 5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.isCandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimer()
	}
}

// Caller ensure lock acquired.
func (rf *Raft) isCandidateLogUpToDate(candidateTerm int, candidateIdx int) bool {
	idx := rf.lastLogicalLogIndex()
	term := rf.log[rf.toPhysicalIndex(idx)].Term
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	if term != candidateTerm {
		return candidateTerm >= term
	}
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	return candidateIdx >= idx
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
	if args.Term > rf.currentTerm {
		rf.switchToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	if rf.state != follower || args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (5.3)
	if args.PrevLogIndex < rf.lastIncludedIndex || args.PrevLogIndex > rf.lastLogicalLogIndex() { // If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
		reply.ConflictIndex = rf.lastLogicalLogIndex() + 1
		return
	}
	// If a follower does have prevLogIndex in its log, but the term does not match,
	// it should return conflictTerm = log[prevLogIndex].Term,
	// and then search its log for the first index whose entry has term equal to conflictTerm.
	if rf.log[rf.toPhysicalIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[rf.toPhysicalIndex(args.PrevLogIndex)].Term
		for i := rf.lastIncludedIndex + 1; i <= rf.lastLogicalLogIndex(); i++ {
			if rf.log[rf.toPhysicalIndex(i)].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		if reply.ConflictIndex == 0 {
			DPrintf("!!!ERROR!!!")
		}
		return
	}
	if len(args.Entries) > 0 {
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (5.3)
		i := args.PrevLogIndex + 1
		j := 0
		for ; i <= rf.lastLogicalLogIndex(); i, j = i+1, j+1 {
			if j >= len(args.Entries) {
				break
			}
			if rf.log[rf.toPhysicalIndex(i)].Term != args.Entries[j].Term {
				if i <= rf.commitIndex {
					DPrintf("Truncating commited logs!!!")
				}
				rf.log = rf.log[:rf.toPhysicalIndex(i)]
				break
			}
		}
		if j < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[j:]...) // 4. Append any new entries not already in the log
		}
		rf.persist()
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIdx := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = Min(args.LeaderCommit, lastNewEntryIdx)
		rf.applyCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) handleVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. No need to handle RequestVote RPC reply if have switched state
	// 2. Old RPC reply, currentTerm has been updated, drop the reply and return
	if rf.state != candidate || args.Term != rf.currentTerm {
		return
	}
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
	if reply.Term > rf.currentTerm {
		rf.switchToFollower(reply.Term)
		return
	}
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > (len(rf.peers) / 2) {
			rf.state = leader

			// Reinitialize nextIndex[] and matchIndex[] after election
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = rf.lastLogicalLogIndex() + 1
			}
		}
	}
}

// Caller ensure lock acquired.
func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (retry bool) {
	if rf.state != leader || args.Term != rf.currentTerm {
		return false
	}
	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.updateCommitIndexUnlocked()
		return false
	}
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
	if reply.Term > rf.currentTerm {
		rf.switchToFollower(reply.Term)
		return false
	}
	// Follower didn't set ConflictIndex, don't perform optimization
	if reply.ConflictIndex == 0 {
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
		return true
	}
	// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
	// If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
	for i := rf.lastLogicalLogIndex(); reply.ConflictTerm != -1 && i > rf.lastIncludedIndex; i-- {
		if rf.log[rf.toPhysicalIndex(i)].Term == reply.ConflictTerm {
			rf.nextIndex[server] = i + 1
			return true
		}
	}
	// If it does not find an entry with that term, it should set nextIndex = conflictIndex.
	rf.nextIndex[server] = reply.ConflictIndex
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.switchToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if rf.state != follower || args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	// If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
	if args.LastIncludedIndex <= rf.lastLogicalLogIndex() && rf.log[rf.toPhysicalIndex(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		rf.log = rf.log[rf.toPhysicalIndex(args.LastIncludedIndex):]
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	rf.persistWithSnapshot(args.Data)

	if rf.lastApplied > rf.lastIncludedIndex {
		return
	}
	msg := ApplyMsg{
		CommandValid: false,
		Snapshot:     args.Data,
	}
	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(msg)
}

// Caller ensures lock acquired.
func (rf *Raft) handleInstallSnapshotReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state != leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.switchToFollower(reply.Term)
		return
	}
	rf.matchIndex[server] = rf.lastIncludedIndex
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	rf.updateCommitIndexUnlocked()
	return
}
