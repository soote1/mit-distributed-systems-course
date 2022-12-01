package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	task := GetMapTask()
	intermediateKeyValues := ProcessMapTask(task, mapf)
	log.Printf("intermediate key-values %v", intermediateKeyValues)
}

func ProcessMapTask(t Task, mapf func(string, string) []KeyValue) []KeyValue {
	// open input file
	file, err := os.Open(t.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", t.InputFile)
	}
	defer file.Close()

	// load split content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", t.InputFile)
	}

	// run map function
	intermediateKeyValues := mapf(t.InputFile, string(content))

	// return sorted list of intermediate key-values
	sort.Sort(ByKey(intermediateKeyValues))
	return intermediateKeyValues
}

func GetMapTask() Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	args.Worker = strconv.Itoa(os.Getuid())
	call("Coordinator.GetMapTask", &args, &reply)

	log.Printf("map task: %v", reply.Task)

	return reply.Task
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
