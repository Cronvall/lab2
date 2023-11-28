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

type TaskArgs struct {
	WorkerID int
}

type TaskReply struct {
	Task      string
	FileID    string
	NReduce   int
	JobNumber int
	NFiles    int
}

type SubmitTaskArgs struct {
	WorkerID int
	FileID   string
	Task     string
}

type SubmitTaskReply struct {
	OK bool
}

type KillWorker struct {
	FileID string
	Task   string
}

type KillWorkerReply struct {
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
