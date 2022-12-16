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
	"time"
)

type MapTaskState struct {
	state map[string]string
	mu    sync.Mutex
}

type ReduceTaskState struct {
	state map[string]string
	mu    sync.Mutex
}

type ReduceTaskLocations struct {
	locations map[string][]string
	mu        sync.Mutex
}

type Coordinator struct {
	mapSplits           []string
	mapTasks            chan Task
	reduceTasks         chan Task
	nReduceTasks        int
	reduceTaskLocations ReduceTaskLocations
	mapTaskState        MapTaskState
	reduceTaskState     ReduceTaskState
	state               string
	done                chan bool
}

func (c *Coordinator) handleState() {
	for {
		log.Printf("coordinator.state: %v", c.state)
		switch c.state {
		case "initial":
			c.mapTaskState.mu.Lock()
			mapTasks := CreateMapTasks(c.mapSplits)
			c.mapTaskState.state = make(map[string]string)
			for _, t := range mapTasks {
				c.mapTasks <- t
				c.mapTaskState.state[t.Id] = "pending"
			}
			c.mapTaskState.mu.Unlock()
			c.state = "map"
		case "map":
			c.mapTaskState.mu.Lock()
			mapFinished := true
			for _, state := range c.mapTaskState.state {
				if state != "done" {
					mapFinished = false
					break
				}
			}
			c.mapTaskState.mu.Unlock()

			if mapFinished {
				c.reduceTaskState.mu.Lock()
				c.reduceTaskState.state = make(map[string]string)
				for taskId, inputs := range c.reduceTaskLocations.locations {
					t := Task{Type: "reduce", Id: taskId, Inputs: inputs}
					c.reduceTasks <- t
					c.reduceTaskState.state[t.Id] = "pending"
				}
				c.reduceTaskState.mu.Unlock()
				c.state = "reduce"
			}
		case "reduce":
			reduceFinished := true
			c.reduceTaskState.mu.Lock()
			for _, state := range c.reduceTaskState.state {
				if state != "done" {
					reduceFinished = false
					break
				}
			}
			c.reduceTaskState.mu.Unlock()

			if reduceFinished {
				c.state = "finished"
			}
		case "finished":
			log.Print("MapReduce process finished")
			c.done <- true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs,
	reply *ReduceTaskDoneReply) error {
	c.reduceTaskState.mu.Lock()
	defer c.reduceTaskState.mu.Unlock()
	c.reduceTaskState.state[args.TaskId] = "done"
	return nil
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

	c.reduceTaskLocations.mu.Lock()
	defer c.reduceTaskLocations.mu.Unlock()

	for _, location := range args.ReduceTaskLocations {
		reduceTaskId := strings.Split(location, "-")[2]
		c.reduceTaskLocations.locations[reduceTaskId] = append(
			c.reduceTaskLocations.locations[reduceTaskId], location)
	}

	c.mapTaskState.mu.Lock()
	defer c.mapTaskState.mu.Unlock()
	c.mapTaskState.state[args.MapTaskId] = "done"

	return nil
}

func (c *Coordinator) GetReduceTask(args *GetTaskArgs,
	reply *GetTaskReply) error {
	log.Printf("Reduce task requested by %v", args.Worker)

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
		reply.Task = nil
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
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func CreateMapTasks(files []string) []Task {
	var tasks []Task

	for i, file := range files {
		t := Task{Type: "map", Inputs: []string{file}, Id: strconv.Itoa(i)}
		tasks = append(tasks, t)
	}

	return tasks
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduceTasks: nReduce,
		mapSplits:    files,
		state:        "initial",
		mapTasks:     make(chan Task, len(files)),
		reduceTasks:  make(chan Task, nReduce),
		done:         make(chan bool, 1),
	}
	c.reduceTaskLocations.locations = make(map[string][]string)
	go c.handleState()
	c.server()
	return &c
}
