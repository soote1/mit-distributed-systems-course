package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

type MapTaskState struct {
	state map[string]string
	mu    sync.Mutex
}

type Coordinator struct {
	mapTasks            chan Task
	reduceTasks         chan Task
	nReduceTasks        int
	reduceTaskLocations map[string][]string
	mapTaskState        MapTaskState
}

func (c *Coordinator) GetMapTask(args *GetTaskArgs,
	reply *GetTaskReply) error {
	log.Printf("Map task requested by %v", args.Worker)
	select {
	case t, ok := <-c.mapTasks:
		if ok {
			log.Printf("Serving map task %v", t)
			reply.Task = &t
			c.mapTaskState.mu.Lock()
			defer c.mapTaskState.mu.Unlock()
			c.mapTaskState.state[t.Id] = "in progress"
		} else {
			log.Printf("Map tasks channel was closed")
		}
	default:
		log.Printf("No map task is available")
		reply.Task = nil
	}
	return nil
}

func (c *Coordinator) GetNReduceTasks(args *GetNReduceTasksArgs,
	reply *GetNReduceTasksReply) error {
	reply.Value = c.nReduceTasks
	return nil
}

func (c *Coordinator) SaveReduceTaskLocations(args *SaveReduceTasksArgs,
	reply *SaveReduceTaskReply) error {
	log.Printf("Saving reduce tasks locations from worker %v", args.Worker)

	if c.reduceTaskLocations == nil {
		log.Print("Initializing reduce task-locations map")
		c.reduceTaskLocations = make(map[string][]string)
	}

	for _, location := range args.ReduceTaskLocations {
		reduceTaskId := strings.Split(location, "-")[2]
		c.reduceTaskLocations[reduceTaskId] = append(
			c.reduceTaskLocations[reduceTaskId], location)
	}

	c.mapTaskState.mu.Lock()
	defer c.mapTaskState.mu.Unlock()
	c.mapTaskState.state[args.MapTaskId] = "done"

	return nil
}

func (c *Coordinator) GetReduceTask(args *GetTaskArgs,
	reply *GetTaskReply) error {
	log.Printf("Reduce task requested by %v", args.Worker)

	c.mapTaskState.mu.Lock()
	defer c.mapTaskState.mu.Unlock()
	mapFinished := true
	log.Printf("mapTaskState %v", c.mapTaskState.state)
	for taskId, state := range c.mapTaskState.state {
		if state != "done" {
			log.Printf("Map task not finished yet: %v", taskId)
			mapFinished = false
		}
	}

	if mapFinished {
		select {
		case t, ok := <-c.reduceTasks:
			if ok {
				log.Printf("Serving reduce task %v", t)
				reply.Task = &t
			} else {
				log.Printf("Reduce tasks channel was closed")
			}
		default:
			log.Printf("No reduce task is available")
			log.Printf("Reduce task-locations %v", c.reduceTaskLocations)
			reply.Task = nil
		}
	} else {
		log.Print("Map phase not finished yet")
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func CreateMapTasks(files []string) []Task {
	var tasks []Task

	for i, file := range files {
		t := Task{Type: "map", Inputs: []string{file}, Id: strconv.Itoa(i)}
		tasks = append(tasks, t)
	}

	return tasks
}

func CreateMapTasksChannel(tasks []Task, size int) chan Task {
	mapTasksChan := make(chan Task, size)

	for _, mapTask := range tasks {
		mapTasksChan <- mapTask
	}

	return mapTasksChan
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduceTasks = nReduce
	mapTasks := CreateMapTasks(files)
	c.mapTaskState.mu.Lock()
	defer c.mapTaskState.mu.Unlock()
	c.mapTaskState.state = make(map[string]string)
	for _, t := range mapTasks {
		c.mapTaskState.state[t.Id] = "pending"
	}
	c.mapTasks = CreateMapTasksChannel(mapTasks, 100)
	c.server()
	return &c
}
