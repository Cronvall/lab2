package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//lint:ignore U1000 Ignore unused function temporarily for debugging

type Coordinator struct {
	//Number of files
	files []string
	//Store the different splits
	//Execute the mapping and store its status
	//Partitioned according to number of nReduces
	// 0 = IDLE
	// 1 = IN-PROGRESS
	// 2 = COMPLETED
	partitionedFiles map[string]int
	//Store the mapped file after status == 1
	intermediateFiles []string
	//Execute the reducing and store its status
	// 0 = IDLE
	// 1 = IN-PROGRESS
	// 2 = COMPLETED
	reduceFiles map[string]int
	//Id and completed tasks for a worker
	worker map[int][]string
	//Check if all maps are done
	mapDone bool
	//nReduce
	nReduce int
	//lock
	mu sync.Mutex
	//Tracker of how many reduce tasks has been done
	reduceTracker int
	//Tracker of how many idle task
	mapTracker int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {

	reduceDone := c.Done()

	c.mu.Lock()
	defer c.mu.Unlock()

	mapDone := c.checkMapDone()
	//fmt.Println("MAP: ", c.partitionedFiles)

	//fmt.Println("REDUCE: ", c.reduceFiles)

	if !mapDone {
		for fileID, state := range c.partitionedFiles {
			if state == 0 {
				reply.Task = "map"
				reply.FileID = fileID
				reply.JobNumber = c.mapTracker
				c.mapTracker++
				c.partitionedFiles[fileID] = 1
				//c.worker[args.WorkerID] = fileID
				reply.NReduce = c.nReduce
				go c.checkTime(fileID, "map")
				return nil
			}
		}
	} else if mapDone {
		for fileID, state := range c.reduceFiles {
			if state == 0 {
				reply.Task = "reduce"
				reply.FileID = fileID
				reply.NReduce = c.nReduce
				reply.JobNumber = c.reduceTracker
				reply.NFiles = len(c.files)
				c.reduceTracker++
				c.reduceFiles[fileID] = 1
				//c.worker[args.WorkerID] = fileID
				go c.checkTime(fileID, "reduce")

				return nil
			}
		}
	}

	if reduceDone {
		os.Exit(4)
	}

	fmt.Println(c.mapDone)
	fmt.Println("return")
	return nil
}

func (c *Coordinator) checkTime(fileID, task string) {

	if task == "map" {
		c.mu.Lock()
		copy := c.partitionedFiles[fileID]
		c.mu.Unlock()
		time.Sleep(10 * time.Second)
		c.mu.Lock()
		if c.partitionedFiles[fileID] == copy {
			c.killWorker(fileID, task)
		}
		c.mu.Unlock()

	} else if task == "reduce" {
		c.mu.Lock()
		copy := c.reduceFiles[fileID]
		c.mu.Unlock()
		time.Sleep(10 * time.Second)
		c.mu.Lock()
		if c.reduceFiles[fileID] == copy {
			c.killWorker(fileID, task)
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) killWorker(fileID, task string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if task == "map" {
		c.partitionedFiles[fileID] = 0
	} else if task == "reduce" {
		c.reduceFiles[fileID] = 0
	}
}

func (c *Coordinator) KillWorker(args *KillWorker, reply *KillWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fileID := args.FileID
	task := args.Task
	if task == "map" {
		c.partitionedFiles[fileID] = 0
	} else if task == "reduce" {
		c.reduceFiles[fileID] = 0
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":8080")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) SubmitJob(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task == "map" {
		c.worker[args.WorkerID] = append(c.worker[args.WorkerID], args.FileID)
		c.partitionedFiles[args.FileID] = 2
		c.reduceFiles[args.FileID] = 0
		reply.OK = true
		return nil
	} else if args.Task == "reduce" {
		c.worker[args.WorkerID] = append(c.worker[args.WorkerID], args.FileID)
		c.reduceFiles[args.FileID] = 2
		reply.OK = true

		return nil
	}
	return nil
}

func (c *Coordinator) checkMapDone() bool {

	for _, state := range c.partitionedFiles {
		if state != 2 {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.reduceFiles) == 0 {
		return false
	}
	for _, state := range c.reduceFiles {
		if state != 2 {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		partitionedFiles:  make(map[string]int),
		intermediateFiles: make([]string, 0),
		reduceFiles:       make(map[string]int),
		worker:            make(map[int][]string),
		mapDone:           false,
		nReduce:           nReduce,
		reduceTracker:     0,
		mapTracker:        0,
	}

	for _, file := range files {
		//fileName := strings.Split(file, ".")[2][1:]
		fileName := file[:len(file)-4]
		c.partitionedFiles[fileName] = 0
	}

	// Your code here.

	c.server()
	return &c
}
