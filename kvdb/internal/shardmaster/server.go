package shardmaster

import (
	"kvdb/internal/labgob"
	"kvdb/internal/labrpc"
	"kvdb/internal/raft"
	"math"
	"sync"
	"time"
)

type void struct{}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	killCh           chan void
	lastAppliedIndex int
	lastAppliedSeq   map[int64]int64
	chanMap          map[int]chan Op
	configs          []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Clerk  int64
	Seq    int64
	Action string

	Args interface{}
}

func (sm *ShardMaster) tryStart(action string, args RPCArgs, reply RPCReply) (channel chan Op, index int, ok bool) {
	reply.SetWrongLeader(false)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.SetWrongLeader(true)
		return nil, 0, false
	}
	if lastAppliedSeq, ok := sm.lastAppliedSeq[args.GetClerk()]; ok {
		if lastAppliedSeq >= args.GetSeq() {
			return nil, 0, false
		}
	}
	commandIndex, _, isLeader := sm.rf.Start(Op{Clerk: args.GetClerk(), Seq: args.GetSeq(), Action: action, Args: args})
	if !isLeader {
		reply.SetWrongLeader(true)
		return nil, 0, false
	}
	ch, ok := sm.chanMap[commandIndex]
	if !ok {
		ch = make(chan Op, 1)
		sm.chanMap[commandIndex] = ch
	}
	return ch, commandIndex, true
}

func (sm *ShardMaster) waitRaft(args RPCArgs, reply RPCReply, action string, ch chan Op, commandIndex int, callback func(args RPCArgs)) {
	select {
	case <-time.After(time.Second):
		reply.SetWrongLeader(true)

	case op := <-ch:
		sm.mu.Lock()
		defer sm.mu.Unlock()
		close(ch)
		delete(sm.chanMap, commandIndex)
		if op.Clerk == args.GetClerk() && op.Seq == args.GetSeq() && op.Action == action {
			if callback != nil {
				callback(args)
			}
			return
		}
		reply.SetWrongLeader(true)
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	ch, commandIndex, ok := sm.tryStart("Join", *args, reply)
	if !ok {
		return
	}
	sm.waitRaft(args, reply, "Join", ch, commandIndex, nil)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	ch, commandIndex, ok := sm.tryStart("Leave", *args, reply)
	if !ok {
		return
	}
	sm.waitRaft(args, reply, "Leave", ch, commandIndex, nil)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	ch, commandIndex, ok := sm.tryStart("Move", *args, reply)
	if !ok {
		return
	}
	sm.waitRaft(args, reply, "Move", ch, commandIndex, nil)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	ch, commandIndex, ok := sm.tryStart("Query", *args, reply)
	if !ok {
		return
	}
	sm.waitRaft(args, reply, "Query", ch, commandIndex, func(args RPCArgs) {
		a := args.(*QueryArgs)
		if a.Num >= 0 && a.Num < len(sm.configs) {
			reply.Config = sm.configs[a.Num]
		} else {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
	})
}

// Kill kills the ShardMaster instance.
// You are not required to do anything in Kill(), but it might be convenient to (for example) turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.killCh)
}

// Raft returns the underlying Raft instance.
// Needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// StartServer starts the ShardMaster.
//   servers[] contains the ports of the set of servers that will cooperate via Raft to form the fault-tolerant shardmaster service.
//   me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := ShardMaster{
		me:             me,
		configs:        make([]Config, 1),
		applyCh:        make(chan raft.ApplyMsg),
		killCh:         make(chan void),
		lastAppliedSeq: make(map[int64]int64),
		chanMap:        make(map[int]chan Op),
	}
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	go sm.applyMsgs()
	return &sm
}

func (sm *ShardMaster) applyMsgs() {
	for {
		select {
		case <-sm.killCh:
			return

		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				continue
			}

			command := msg.Command.(Op)
			sm.mu.Lock()
			lastAppliedSeq, ok := sm.lastAppliedSeq[command.Clerk]
			if !ok || lastAppliedSeq < command.Seq {
				switch command.Action {
				case "Join":
					args := command.Args.(JoinArgs)
					sm.join(&args)
				case "Leave":
					args := command.Args.(LeaveArgs)
					sm.leave(&args)
				case "Move":
					args := command.Args.(MoveArgs)
					sm.move(&args)
				case "Query":
				default:
				}
				sm.lastAppliedSeq[command.Clerk] = command.Seq
			}
			sm.lastAppliedIndex = msg.CommandIndex
			ch, ok := sm.chanMap[msg.CommandIndex]
			sm.mu.Unlock()
			if ok {
				ch <- command
			}
		}
	}
}

func (sm *ShardMaster) join(args *JoinArgs) {
	next := sm.createNextConfig()
	oldGroups := getGroups(next)
	for gid, servers := range args.Servers {
		next.Groups[gid] = make([]string, len(servers))
		copy(next.Groups[gid], servers)

		// rebalance
		m := groupByGID(next)
		avg := NShards / len(next.Groups)
		for i := 0; i < avg; i++ {
			maxGID := getGIDwithMaxShards(m, oldGroups)
			next.Shards[m[maxGID][0]] = gid
			m[maxGID] = m[maxGID][1:]
		}
	}
	sm.configs = append(sm.configs, *next)
}

func (sm *ShardMaster) leave(args *LeaveArgs) {
	next := sm.createNextConfig()
	for _, gid := range args.GIDs {
		if _, ok := next.Groups[gid]; ok {
			delete(next.Groups, gid)
			if len(next.Groups) == 0 {
				next.Shards = [NShards]int{}
				break
			}

			// rebalance
			m := groupByGID(next)
			shards := m[gid]
			delete(m, gid)
			for _, shard := range shards {
				minGID := getGIDwithMinShards(m)
				next.Shards[shard] = minGID
				m[minGID] = append(m[minGID], shard)
			}
		}
	}
	sm.configs = append(sm.configs, *next)
}

func (sm *ShardMaster) move(args *MoveArgs) {
	next := sm.createNextConfig()
	if _, ok := next.Groups[args.GID]; ok {
		next.Shards[args.Shard] = args.GID
		sm.configs = append(sm.configs, *next)
	}
}

func (sm *ShardMaster) createNextConfig() *Config {
	previous := sm.configs[len(sm.configs)-1]
	next := Config{
		Num:    previous.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	copy(next.Shards[:], previous.Shards[:])
	for k, v := range previous.Groups {
		next.Groups[k] = append(make([]string, 0), v...)
	}
	return &next
}

// gid -> shards
func groupByGID(config *Config) map[int][]int {
	m := make(map[int][]int)
	for gid := range config.Groups {
		m[gid] = make([]int, 0)
	}
	for i, gid := range config.Shards {
		m[gid] = append(m[gid], i)
	}
	return m
}

func getGIDwithMaxShards(m map[int][]int, old map[int]void) (gid int) {
	count := -1
	gid = 0
	for k, v := range m {
		if len(v) > count {
			gid = k
			count = len(v)
		} else if len(v) == count {
			if _, ok := old[k]; !ok {
				gid = k
			}
		}
	}
	return
}

func getGIDwithMinShards(m map[int][]int) (gid int) {
	count := math.MaxInt32
	gid = 0
	for k, v := range m {
		if len(v) < count {
			gid = k
			count = len(v)
		}
	}
	return
}

func getGroups(config *Config) map[int]void {
	groups := make(map[int]void)
	for gid := range config.Groups {
		groups[gid] = void{}
	}
	return groups
}
