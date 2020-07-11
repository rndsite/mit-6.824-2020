package raft

type (
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

// RequestVote RPC handler.
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
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
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.isCandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimer()
	}
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
		rf.switchToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.state != follower || args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.lastLogIndex() { // If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
		reply.ConflictIndex = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // If a follower does have prevLogIndex in its log, but the term does not match, it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
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
	if len(args.Entries) > 0 {
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		i := args.PrevLogIndex + 1
		j := 0
		for ; i < len(rf.log); i, j = i+1, j+1 {
			if j >= len(args.Entries) {
				break
			}
			if rf.log[i].Term != args.Entries[j].Term || rf.log[i].Command != args.Entries[j].Command {
				if i <= rf.commitIndex {
					DPrintf("Truncating commited logs!!!")
				}
				rf.log = rf.log[:i]
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
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.switchToFollower(reply.Term)
		rf.resetElectionTimer()
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
				rf.nextIndex[i] = rf.lastLogIndex() + 1
			}
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		if reply.Term > rf.currentTerm {
			rf.switchToFollower(reply.Term)
			rf.resetElectionTimer()
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
	}
}
