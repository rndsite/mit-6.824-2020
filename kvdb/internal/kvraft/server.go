package kvraft

import (
	"bytes"
	"kvdb/internal/labgob"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"kvdb/internal/labrpc"

	"kvdb/internal/raft"
)

type (
	void struct{}
)

var member void

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client int64
	Seq    int64
	Action string
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	term             int
	killCh           chan void
	db               map[string]string
	lastAppliedSeq   map[int64]int64
	lastAppliedIndex int
	chanMap          map[int]chan Op
	persister        *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	reply.Err = OK
	if lastAppliedSeq, ok := kv.lastAppliedSeq[args.Client]; ok {
		if lastAppliedSeq >= args.Seq {
			v, ok := kv.db[args.Key]
			kv.mu.Unlock()
			if ok {
				reply.Value = v
				return
			}
			reply.Err = ErrNoKey
			return
		}
	}
	commandIndex, _, isLeader := kv.rf.Start(Op{Client: args.Client, Seq: args.Seq, Action: "Get", Key: args.Key})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch, ok := kv.chanMap[commandIndex]
	if !ok {
		ch = make(chan Op, 1)
		kv.chanMap[commandIndex] = ch
	}
	kv.mu.Unlock()
	select {
	case <-time.After(time.Second):
		reply.Err = ErrRaftTimeout
	case op := <-ch:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		close(ch)
		delete(kv.chanMap, commandIndex)
		if op.Client == args.Client && op.Seq == args.Seq && op.Action == "Get" && op.Key == args.Key {
			v, ok := kv.db[args.Key]
			if ok {
				reply.Value = v
				return
			}
			reply.Err = ErrNoKey
			return
		}
		reply.Err = ErrNoAgreement
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	reply.Err = OK
	if lastAppliedSeq, ok := kv.lastAppliedSeq[args.Client]; ok {
		if lastAppliedSeq >= args.Seq {
			kv.mu.Unlock()
			return
		}
	}
	commandIndex, _, isLeader := kv.rf.Start(Op{Client: args.Client, Seq: args.Seq, Action: args.Op, Key: args.Key, Value: args.Value})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch, ok := kv.chanMap[commandIndex]
	if !ok {
		ch = make(chan Op, 1)
		kv.chanMap[commandIndex] = ch
	}
	kv.mu.Unlock()
	select {
	case <-time.After(time.Second):
		reply.Err = ErrRaftTimeout
	case op := <-ch:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		close(ch)
		delete(kv.chanMap, commandIndex)
		if op.Client == args.Client && op.Seq == args.Seq && op.Action == args.Op && op.Key == args.Key && op.Value == args.Value {
			return
		}
		// Different command got at same index
		reply.Err = ErrNoAgreement
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.killCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer starts a Key/Value server.
// Args:
//    servers[]:    contains the ports of the set of servers that will cooperate via Raft to form the fault-tolerant key/value service.
//    me:           the index of the current server in servers[].
//    persister:    the underlying persister to store snapshot.
//                  The k/v server should store snapshots through the underlying Raft implementation,
//				    which should call persister.SaveStateAndSnapshot() to atomically save the Raft state along with the snapshot.
//    maxraftstate: the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
//                  in order to allow Raft to garbage-collect its log.
//                  if maxraftstate is -1, you don't need to snapshot.
//
// StartKVServer() must return quickly, so it should start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &KVServer{
		me:             me,
		applyCh:        make(chan raft.ApplyMsg),
		maxraftstate:   maxraftstate,
		killCh:         make(chan void),
		db:             make(map[string]string),
		lastAppliedSeq: make(map[int64]int64),
		chanMap:        make(map[int]chan Op),
		persister:      persister,
	}

	kv.readPersist(kv.persister.ReadSnapshot())
	go kv.applyMsgLoop()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	return kv
}

func (kv *KVServer) applyMsgLoop() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				command := msg.Command.(Op)
				kv.mu.Lock()
				lastAppliedSeq, ok := kv.lastAppliedSeq[command.Client]
				if !ok || lastAppliedSeq < command.Seq {
					switch command.Action {
					case "Put":
						kv.db[command.Key] = command.Value
					case "Append":
						kv.db[command.Key] += command.Value
					default:
					}
					kv.lastAppliedSeq[command.Client] = command.Seq
				}
				kv.lastAppliedIndex = msg.CommandIndex
				ch, ok := kv.chanMap[msg.CommandIndex]
				kv.mu.Unlock()
				if ok {
					ch <- command
				}
				if kv.needSnapshot() {
					kv.takeSnapshot()
				}
			} else {
				kv.readPersist(msg.Snapshot)
			}

		case <-kv.killCh:
			return
		}
	}
}

func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	if kv.persister.RaftStateSize() >= kv.maxraftstate {
		return true
	}
	return false
}

func (kv *KVServer) takeSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.lastAppliedSeq)
	e.Encode(kv.lastAppliedIndex)
	snapshot := w.Bytes()
	lastAppliedIndex := kv.lastAppliedIndex
	kv.mu.Unlock()
	kv.rf.TakeSnapshot(lastAppliedIndex, snapshot)
}

func (kv *KVServer) readPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	db := map[string]string{}
	lastAppliedSeq := map[int64]int64{}
	var lastAppliedIndex int
	if d.Decode(&db) != nil || d.Decode(&lastAppliedSeq) != nil || d.Decode(&lastAppliedIndex) != nil {
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		kv.db = db
		kv.lastAppliedSeq = lastAppliedSeq
		kv.lastAppliedIndex = lastAppliedIndex
	}
}
