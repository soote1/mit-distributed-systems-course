package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
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

type MapFunction func(string, string) []KeyValue
type ReduceFunction func(string, []string) string

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
func Worker(mapf MapFunction, reducef ReduceFunction) {
	nReduceTasks := GetNReduceTasks()
	for {
		mapTask := GetMapTask()
		if mapTask != nil {
			ProcessMapTask(mapTask, mapf, nReduceTasks)
		} else {
			log.Printf("No map task is available")
			reduceTask := GetReduceTask()
			if reduceTask != nil {
				ProcessReduceTask(reduceTask, reducef)
			} else {
				log.Print("No reduce task is available")
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func GetReduceTask() *Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	args.Worker = strconv.Itoa(os.Getuid())

	log.Printf("Requesting reduce task")
	call("Coordinator.GetReduceTask", &args, &reply)

	return reply.Task
}

func LoadReduceTaskInput(fileNames []string) []KeyValue {
	var keyValues []KeyValue

	log.Printf("Loading reduce content from input files %v", fileNames)

	for _, fileName := range fileNames {
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
		if err != nil {
			log.Fatalf("Cant open intermediate file %v", err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				} else {
					log.Fatalf("Json decoding error found: %v", err)
				}
			}
			keyValues = append(keyValues, kv)
		}
	}

	log.Printf("Loaded %v key values", len(keyValues))

	return keyValues
}

func RunReduceFunction(input []KeyValue, reducef ReduceFunction) []string {
	var result []string

	log.Printf("Running reduce function")

	i := 0
	for i < len(input) {
		j := i + 1
		for j < len(input) && input[j].Key == input[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, input[k].Value)
		}
		output := reducef(input[i].Key, values)

		result = append(result, input[i].Key+" "+output+"\n")

		i = j
	}

	log.Printf("Generated %v results", len(result))

	return result
}

func SaveReduceOutput(fileName string, content []string) {
	log.Printf("Saving reduce output in %v", fileName)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("cant open intermediate file %v", err)
	}
	defer file.Close()

	log.Printf("Writing %v strings into %v", len(content), fileName)
	for i := 0; i < len(content); i++ {
		file.WriteString(content[i])
	}
}

func NotifyReduceTaskDone(taskId string) {
	args := ReduceTaskDoneArgs{}
	reply := ReduceTaskDoneReply{}

	log.Printf("Notifying reduce task %v is done", taskId)

	args.Worker = strconv.Itoa(os.Geteuid())
	args.TaskId = taskId
	call("Coordinator.ReduceTaskDone", &args, &reply)
}

func ProcessReduceTask(reduceTask *Task, reducef ReduceFunction) {
	log.Printf("Processing reduce task %v", reduceTask)
	mapTaskId := strings.Split(reduceTask.Id, "-")[0]
	outFileName := "mr-out-" + mapTaskId

	input := LoadReduceTaskInput(reduceTask.Inputs)
	sort.Sort(ByKey(input))
	reduceOutput := RunReduceFunction(input, reducef)
	SaveReduceOutput(outFileName, reduceOutput)
	NotifyReduceTaskDone(reduceTask.Id)
}

func ProcessMapTask(mapTask *Task, mapf MapFunction, nReduceTasks int) {
	log.Printf("Processing map task %v", mapTask)
	keyValues := RunMapFunction(mapTask, mapf)
	reduceTasks := GenerateReduceTasks(mapTask.Id, keyValues, nReduceTasks)
	SaveReduceTaskLocations(mapTask.Id, reduceTasks)
}

func GenerateReduceTasks(mapTaskId string, kvs []KeyValue,
	nReduceTasks int) []string {
	var reduceTasksLocations []string
	reduceTaskIds := make(map[string]bool)

	log.Printf("Generating reduce tasks")

	for _, kv := range kvs {
		reduceTaskId := strconv.Itoa(ihash(kv.Key) % nReduceTasks)
		reduceTaskId = mapTaskId + "-" + reduceTaskId
		fileName := "mr-" + reduceTaskId

		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY,
			0644)
		if err != nil {
			log.Fatalf("cant open intermediate file %v", err)
		}

		enc := json.NewEncoder(file)
		if err := enc.Encode(&kv); err != nil {
			file.Close()
			log.Fatalf("error while trying to write content in json format %v", err)
		}

		if err := file.Close(); err != nil {
			log.Fatalf("error while closing intermediate file %v", err)
		}

		if _, ok := reduceTaskIds[reduceTaskId]; !ok {
			reduceTasksLocations = append(reduceTasksLocations, fileName)
		}

		reduceTaskIds[reduceTaskId] = true
	}

	return reduceTasksLocations
}

func RunMapFunction(t *Task, mapf MapFunction) []KeyValue {
	// open input file
	log.Printf("Running map function")
	inputFile := t.Inputs[0]
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("cannot open %v", inputFile)
	}
	defer file.Close()

	// load split content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile)
	}

	// run map function
	intermediateKeyValues := mapf(inputFile, string(content))

	return intermediateKeyValues
}

func GetMapTask() *Task {
	log.Printf("Requesting map task")
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	args.Worker = strconv.Itoa(os.Getuid())
	call("Coordinator.GetMapTask", &args, &reply)

	return reply.Task
}

func GetNReduceTasks() int {
	args := GetNReduceTasksArgs{}
	reply := GetNReduceTasksReply{}

	args.Worker = strconv.Itoa(os.Geteuid())
	call("Coordinator.GetNReduceTasks", &args, &reply)

	return reply.Value
}

func SaveReduceTaskLocations(mapTaskId string, tasks []string) {
	args := SaveReduceTasksArgs{}
	reply := SaveReduceTaskReply{}

	log.Printf("Sending reduce tasks to coordinator")

	args.Worker = strconv.Itoa(os.Geteuid())
	args.ReduceTaskLocations = tasks
	args.MapTaskId = mapTaskId
	call("Coordinator.SaveReduceTaskLocations", &args, &reply)
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
