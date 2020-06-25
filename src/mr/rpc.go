package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Args ...
type Args struct {
	TaskType  TaskType
	WorkerID  string
	MapperID  int
	ReducerID int
	Files     []string
}

// Reply ...
type Reply struct {
	TaskType  TaskType
	MapperID  int
	ReducerID int
	Filename  string
	NMap      int
	NReduce   int
	Files     []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
