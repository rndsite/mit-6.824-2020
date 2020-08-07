package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type RPCArgs interface {
	GetClerk() int64
	GetSeq() int64
}

type RPCReply interface {
	SetWrongLeader(bool)
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	Clerk int64
	Seq   int64
}

func (j JoinArgs) GetClerk() int64 {
	return j.Clerk
}

func (j JoinArgs) GetSeq() int64 {
	return j.Seq
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

func (j *JoinReply) SetWrongLeader(wrong bool) {
	j.WrongLeader = wrong
}

type LeaveArgs struct {
	GIDs []int

	Clerk int64
	Seq   int64
}

func (l LeaveArgs) GetClerk() int64 {
	return l.Clerk
}

func (l LeaveArgs) GetSeq() int64 {
	return l.Seq
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

func (l *LeaveReply) SetWrongLeader(wrong bool) {
	l.WrongLeader = wrong
}

type MoveArgs struct {
	Shard int
	GID   int

	Clerk int64
	Seq   int64
}

func (m MoveArgs) GetClerk() int64 {
	return m.Clerk
}

func (m MoveArgs) GetSeq() int64 {
	return m.Seq
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

func (m *MoveReply) SetWrongLeader(wrong bool) {
	m.WrongLeader = wrong
}

type QueryArgs struct {
	Num int // desired config number

	Clerk int64
	Seq   int64
}

func (q QueryArgs) GetClerk() int64 {
	return q.Clerk
}

func (q QueryArgs) GetSeq() int64 {
	return q.Seq
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (q *QueryReply) SetWrongLeader(wrong bool) {
	q.WrongLeader = wrong
}
