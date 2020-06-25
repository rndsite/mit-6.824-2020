package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"

	"github.com/google/uuid"
)

// TaskType ...
type TaskType string

// ...
const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	Exit       TaskType = "exit"
)

type (
	// KeyValue ... Map functions return a slice of KeyValue.
	KeyValue struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	// KeyValues ...
	KeyValues struct {
		Results []KeyValue `json:"results"`
	}

	// ByKey ... for sorting by key.
	ByKey []KeyValue
)

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker ... main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	id := uuid.New().String()

	for {
		args := Args{
			WorkerID: id,
		}
		reply := Reply{}
		if ok := call("Master.GetTask", args, &reply); !ok {
			return
		}

		switch reply.TaskType {
		case MapTask:
			doMapTask(mapf, id, reply.Filename, reply.MapperID, reply.NReduce)
		case ReduceTask:
			doReduceTask(reducef, reply.Files, reply.ReducerID, reply.NMap)
		case Exit:
			return
		default:
			return
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, workerID string, iname string, mapperID int, nReduce int) {
	// Read input file
	ifile, _ := os.Open(iname)
	defer ifile.Close()

	content, _ := ioutil.ReadAll(ifile)

	// Do map function
	kva := mapf(iname, string(content))

	// Store to nReduce buckets
	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reducerIdx := ihash(kv.Key) % nReduce
		intermediate[reducerIdx] = append(intermediate[reducerIdx], kv)
	}

	oFiles := []string{}

	// Write each bucket to file
	for i, kva := range intermediate {
		tmpfile, _ := ioutil.TempFile("", "private-"+strconv.Itoa(i)+"-")

		keyValues := KeyValues{
			Results: kva,
		}

		bytes, _ := json.Marshal(keyValues)
		tmpfile.Write(bytes)
		tmpfile.Close()

		oname := "mr-" + strconv.Itoa(mapperID) + "-" + strconv.Itoa(i) + "-" + workerID
		os.Rename(tmpfile.Name(), oname)
		oFiles = append(oFiles, oname)
	}

	// Report task complete to master
	args := Args{
		TaskType: MapTask,
		MapperID: mapperID,
		WorkerID: workerID,
		Files:    oFiles,
	}
	call("Master.FinishTask", args, nil)
}

func doReduceTask(reducef func(string, []string) string, ifiles []string, reducerID int, nMap int) {
	intermediate := []KeyValue{}

	// Merge all intermediate results from one mapper
	for _, iname := range ifiles {
		ifile, _ := os.Open(iname)
		content, _ := ioutil.ReadAll(ifile)

		keyValues := KeyValues{}
		json.Unmarshal(content, &keyValues)

		intermediate = append(intermediate, keyValues.Results...)
	}

	// Sort the intermediate results by key.
	sort.Sort(ByKey(intermediate))

	tmpfile, _ := ioutil.TempFile("", "private-")
	defer tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	// call Reduce on each distinct key in intermediate[], and print the result to mr-out-<reducerID>.
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(tmpfile.Name(), "mr-out-"+strconv.Itoa(reducerID))

	// Report task complete to master
	args := Args{
		TaskType:  ReduceTask,
		ReducerID: reducerID,
	}
	call("Master.FinishTask", args, nil)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
