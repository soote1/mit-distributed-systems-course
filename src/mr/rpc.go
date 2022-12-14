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

type Task struct {
	Type   string
	Inputs []string
	Id     string
}

type GetTaskArgs struct {
	Worker string
}

type GetTaskReply struct {
	Task *Task
}

type GetNReduceTasksArgs struct {
	Worker string
}

type GetNReduceTasksReply struct {
	Value int
}

type SaveReduceTasksArgs struct {
	ReduceTaskLocations []string
	MapTaskId           string
	Worker              string
}

type SaveReduceTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
