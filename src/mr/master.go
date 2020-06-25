package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	unassigned = "unassigned"
	dispatched = "dispatched"
	done       = "done"
)

// MapTaskMetadata ...
type MapTaskMetadata struct {
	Filename    string
	WorkerID    string
	StartTime   time.Time
	State       string
	OutputFiles []string
}

// ReduceTaskMetadata ...
type ReduceTaskMetadata struct {
	StartTime time.Time
	State     string
}

// Master ...
type Master struct {
	NMap    int
	NReduce int

	MapTaskTracker []MapTaskMetadata
	MapPhaseDone   bool

	MapPhaseLock sync.Mutex
	MapPhaseCond *sync.Cond

	ReduceTaskTracker []ReduceTaskMetadata
	ReducePhaseDone   bool

	ReducePhaseLock sync.Mutex
	ReducePhaseCond *sync.Cond
}

// return true if find one task to dispatch
func (m *Master) dispatchMapTask(args *Args, reply *Reply) bool {
	m.MapPhaseLock.Lock()
	defer m.MapPhaseLock.Unlock()

	for !m.MapPhaseDone {

		for i, task := range m.MapTaskTracker {
			if task.State == unassigned {
				reply.TaskType = MapTask
				reply.Filename = task.Filename
				reply.MapperID = i
				reply.NReduce = m.NReduce

				m.MapTaskTracker[i].State = dispatched
				m.MapTaskTracker[i].WorkerID = args.WorkerID
				m.MapTaskTracker[i].StartTime = time.Now()

				return true
			}
		}

		m.MapPhaseCond.Wait()

	}

	return false
}

// return true if find one task to dispatch
func (m *Master) dispatchReduceTask(args *Args, reply *Reply) bool {
	m.ReducePhaseLock.Lock()
	defer m.ReducePhaseLock.Unlock()

	for !m.ReducePhaseDone {

		for i, task := range m.ReduceTaskTracker {
			if task.State == unassigned {
				reply.TaskType = ReduceTask
				reply.ReducerID = i
				reply.NMap = m.NMap
				reply.Files = []string{}

				// No need to lock
				for _, mapTask := range m.MapTaskTracker {
					reply.Files = append(reply.Files, mapTask.OutputFiles[i])
				}

				m.ReduceTaskTracker[i].State = dispatched
				m.ReduceTaskTracker[i].StartTime = time.Now()

				return true
			}
		}

		m.ReducePhaseCond.Wait()

	}

	return false
}

// GetTask ...
func (m *Master) GetTask(args *Args, reply *Reply) error {
	if got := m.dispatchMapTask(args, reply); got {
		return nil
	}

	if got := m.dispatchReduceTask(args, reply); got {
		return nil
	}

	reply.TaskType = Exit
	return nil
}

// FinishTask ...
func (m *Master) FinishTask(args *Args, _ *Reply) error {
	switch args.TaskType {
	case MapTask:

		m.MapPhaseLock.Lock()
		defer m.MapPhaseLock.Unlock()

		// Check if map phase already finished
		if m.MapPhaseDone {
			return nil
		}

		// Check if some worker has finished this task
		if m.MapTaskTracker[args.MapperID].State == done {
			return nil
		}

		m.MapTaskTracker[args.MapperID].WorkerID = args.WorkerID
		m.MapTaskTracker[args.MapperID].State = done
		m.MapTaskTracker[args.MapperID].OutputFiles = append(m.MapTaskTracker[args.MapperID].OutputFiles, args.Files...)

	case ReduceTask:

		m.ReducePhaseLock.Lock()
		defer m.ReducePhaseLock.Unlock()

		// Check if reduce phase already finished
		if m.ReducePhaseDone {
			return nil
		}

		// Check if some worker has finished this task
		if m.ReduceTaskTracker[args.ReducerID].State == done {
			return nil
		}

		m.ReduceTaskTracker[args.ReducerID].State = done

	default:
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done ...
//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Check if map phase done
	m.MapPhaseLock.Lock()
	if !m.MapPhaseDone {
		finished := true
		for i, task := range m.MapTaskTracker {
			if task.State != done {
				finished = false
			}

			if (task.State == dispatched) && (time.Now().Sub(task.StartTime).Seconds() >= 10) {
				m.MapTaskTracker[i].State = unassigned
			}
		}

		if finished {
			m.MapPhaseDone = true
		}

		m.MapPhaseCond.Broadcast()
	}
	m.MapPhaseLock.Unlock()

	// Check if reduce phase done
	m.ReducePhaseLock.Lock()
	if !m.ReducePhaseDone {
		finished := true
		for i, task := range m.ReduceTaskTracker {
			if task.State != done {
				finished = false
			}

			if (task.State == dispatched) && (time.Now().Sub(task.StartTime).Seconds() >= 10) {
				m.ReduceTaskTracker[i].State = unassigned
			}
		}

		if finished {
			m.ReducePhaseDone = true
		}

		m.ReducePhaseCond.Broadcast()
	} else {
		ret = true
	}
	m.ReducePhaseLock.Unlock()

	return ret
}

// MakeMaster ...
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		NMap:    len(files),
		NReduce: nReduce,

		MapPhaseDone:    false,
		ReducePhaseDone: false,

		MapTaskTracker:    make([]MapTaskMetadata, len(files)),
		ReduceTaskTracker: make([]ReduceTaskMetadata, nReduce),

		MapPhaseLock:    sync.Mutex{},
		ReducePhaseLock: sync.Mutex{},
	}

	m.MapPhaseCond = sync.NewCond(&m.MapPhaseLock)
	m.ReducePhaseCond = sync.NewCond(&m.ReducePhaseLock)

	for i := 0; i < m.NMap; i++ {
		m.MapTaskTracker[i] = MapTaskMetadata{
			Filename:    files[i],
			State:       unassigned,
			OutputFiles: []string{},
		}
	}

	for i := 0; i < m.NReduce; i++ {
		m.ReduceTaskTracker[i] = ReduceTaskMetadata{
			State: unassigned,
		}
	}

	m.server()
	return &m
}
