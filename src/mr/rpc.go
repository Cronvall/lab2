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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MapTaskReply struct {
	FileID  string
	NReduce int
}

type ReduceTaskReply struct {
	ReduceID string
	NMap     int
}

type IntermediateData struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type TaskReply struct {
	FileID   string
	N        int //nReduce or nMap
	TaskType string
}

type TaskArgs struct {
	WorkerID int
	TaskType string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
