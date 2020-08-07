package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"kvdb/internal/labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

// ClerkID ...
var ClerkID int64

func init() {
	ClerkID = 0
}

// Clerk ...
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id  int64
	seq int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk ...
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = atomic.AddInt64(&ClerkID, 1)
	return ck
}

// Query ...
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:   num,
		Clerk: ck.id,
		Seq:   atomic.AddInt64(&ck.seq, 1),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Join ...
func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers: servers,
		Clerk:   ck.id,
		Seq:     atomic.AddInt64(&ck.seq, 1),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Leave ...
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:  gids,
		Clerk: ck.id,
		Seq:   atomic.AddInt64(&ck.seq, 1),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Move ...
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard: shard,
		GID:   gid,
		Clerk: ck.id,
		Seq:   atomic.AddInt64(&ck.seq, 1),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
