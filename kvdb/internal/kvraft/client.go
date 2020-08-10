package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"kvdb/internal/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id     int64
	seq    int64
	leader int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers: servers,
		id:      nrand(),
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	N := int32(len(ck.servers))
	i := atomic.LoadInt32(&ck.leader)
	args := GetArgs{
		Key:    key,
		Client: ck.id,
		Seq:    atomic.AddInt64(&ck.seq, 1),
	}

	for {
		var reply GetReply
		if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); !ok || reply.Err == ErrWrongLeader {
			i = (i + 1) % N
			continue
		}
		atomic.StoreInt32(&ck.leader, int32(i))
		if reply.Err == OK || reply.Err == ErrNoKey {
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	N := int32(len(ck.servers))
	i := atomic.LoadInt32(&ck.leader)
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		Client: ck.id,
		Seq:    atomic.AddInt64(&ck.seq, 1),
	}

	for {
		var reply PutAppendReply
		if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); !ok || reply.Err == ErrWrongLeader {
			i = (i + 1) % N
			continue
		}
		atomic.StoreInt32(&ck.leader, i)
		if reply.Err == OK {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
