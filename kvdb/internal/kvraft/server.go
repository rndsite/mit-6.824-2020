package kvraft

import (
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
	ID     string
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
	killCh           chan bool
	db               map[string]string
	applied          map[string]void
	lastAppliedIndex int
	appliedCond      *sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Err = OK
	if _, ok := kv.applied[args.CommandID]; ok {
		if v, ok := kv.db[args.Key]; ok {
			reply.Value = v
			return
		}
		reply.Err = ErrNoKey
		return
	}
	commandIndex, _, isLeader := kv.rf.Start(Op{
		ID:     args.CommandID,
		Action: "Get",
		Key:    args.Key,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		kv.appliedCond.Wait()
		if kv.killed() {
			reply.Err = ErrKilled
			return
		}
		if _, isLeader = kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if _, ok := kv.applied[args.CommandID]; ok {
			if v, ok := kv.db[args.Key]; ok {
				reply.Value = v
				return
			}
			reply.Err = ErrNoKey
			return
		}
		if kv.lastAppliedIndex >= commandIndex {
			reply.Err = ErrNoAgreement
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Err = OK
	if _, ok := kv.applied[args.CommandID]; ok {
		return
	}
	commandIndex, _, isLeader := kv.rf.Start(Op{
		ID:     args.CommandID,
		Action: args.Op,
		Key:    args.Key,
		Value:  args.Value,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		kv.appliedCond.Wait()
		if kv.killed() {
			reply.Err = ErrKilled
			return
		}
		if _, isLeader = kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if _, ok := kv.applied[args.CommandID]; ok {
			return
		}
		if kv.lastAppliedIndex >= commandIndex {
			reply.Err = ErrNoAgreement
			return
		}
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
	kv.killCh <- true
	kv.appliedCond.Broadcast()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.killCh = make(chan bool)
	kv.db = make(map[string]string)
	kv.applied = make(map[string]void)
	kv.appliedCond = sync.NewCond(&kv.mu)
	kv.lastAppliedIndex = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	go kv.applyMsgLoop()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.checkStateLoop()

	return kv
}

func (kv *KVServer) applyMsgLoop() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				command := msg.Command.(Op)
				kv.mu.Lock()
				if _, ok := kv.applied[command.ID]; !ok {
					switch command.Action {
					case "Put":
						kv.db[command.Key] = command.Value
					case "Append":
						kv.db[command.Key] += command.Value
					default:
					}
					kv.applied[command.ID] = member
				}
				kv.lastAppliedIndex = msg.CommandIndex
				kv.appliedCond.Broadcast()
				kv.mu.Unlock()
			}
		case <-kv.killCh:
			return
		}
	}
}

func (kv *KVServer) checkStateLoop() {
	for {
		select {
		case <-time.After(time.Duration(500) * time.Millisecond):
			kv.mu.Lock()
			term, _ := kv.rf.GetState()
			if term != kv.term {
				kv.appliedCond.Broadcast()
				kv.term = term
			}
			kv.mu.Unlock()
		case <-kv.killCh:
			return
		}
	}
}
