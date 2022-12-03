package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Coordinator struct {
	mapTasks     chan Task
	reduceTasks  chan Task
	nReduceTasks int
}

func (c *Coordinator) GetMapTask(args *GetTaskArgs,
	reply *GetTaskReply) error {
	reply.Task = <-c.mapTasks
	return nil
}

func (c *Coordinator) GetNReduceTasks(args *GetNReduceTasksArgs,
	reply *GetNReduceTasksReply) error {
	reply.Value = c.nReduceTasks
	return nil
}

func (c *Coordinator) SaveReduceTasks(args *SaveReduceTasksArgs,
	reply *SaveReduceTaskReply) error {
	if c.reduceTasks == nil {
		c.reduceTasks = make(chan Task, 100)
	}
	for _, task := range args.ReduceTasks {
		c.reduceTasks <- task
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
		t := Task{Type: "map", InputFile: file, Id: strconv.Itoa(i)}
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
	c.mapTasks = CreateMapTasksChannel(CreateMapTasks(files), 100)
	c.server()
	return &c
}
