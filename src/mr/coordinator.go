package mr

import (
	"fmt"
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
	reduceStatus map[string]int
	//Id and completed tasks for a worker
	worker map[int]string
	//Check if all maps are done
	mapDone    bool
	reduceDone bool
	//nReduce
	nReduce int
	//lock
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestMapTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for fileID, state := range c.partitionedFiles {
		if state == 0 {
			reply.FileID = fileID
			c.partitionedFiles[fileID] = 1
			c.worker[args.WorkerID] = fileID
			reply.N = c.nReduce
			reply.TaskType = "map"
			fmt.Println("return in for")
			return nil
		} else {
			c.mapDone = true
		}
	}
	fmt.Println("return")
	return nil
}

func (c *Coordinator) RequestReduceTask(args *TaskArgs, reply *TaskReply) error {
	// Implement logic to assign reduce tasks to workers
	// ...
	c.mu.Lock()
	defer c.mu.Unlock()

	for reduceID, state := range c.reduceStatus {
		if state == 0 {
			reply.FileID = reduceID
			c.reduceStatus[reduceID] = 1
			c.worker[args.WorkerID] = reduceID
			reply.N = len(c.files)
			reply.TaskType = "reduce"
			fmt.Println("return in for")
			return nil
		}
	}
	return nil
}

func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	// Implement logic to assign reduce tasks to workers
	// ...
	if c.mapDone && !c.reduceDone {
		fmt.Println("REQ Reduce")
		c.RequestReduceTask(args, reply)
	} else if !c.mapDone {
		fmt.Println("REQ Map")
		c.RequestMapTask(args, reply)
	} else {
		fmt.Println("All REQs Done")
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
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	fmt.Println("DONE")

	// Future planning:
	// Check if completed tasks are the same it was for worker 10 seconds earlier, if so,
	// worker has failed

	failBool := make(chan bool)
	failWorker := make(chan int)

	for worker := range c.worker {
		workerCopy := c.worker[worker]
		go func(workerID int) {
			time.Sleep(10 * time.Second)
			if workerCopy == c.worker[workerID] {
				// Bad, wont work fix, want to return what workedID fails and then terminate all prior tasks
				failBool <- true
				failWorker <- workerID
			}
		}(worker)
	}
	if <-failBool {
		//failedWorker := <-failWorker
		//use the ID to remove the prior tasks from the failed worker and assign them to a new one
		return ret
	}
	// Your code here.
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
		reduceStatus:      make(map[string]int),
		worker:            make(map[int]string),
		mapDone:           false,
		reduceDone:        false,
		nReduce:           nReduce,
	}

	for _, file := range files {
		rawFileContent, err := os.ReadFile(file)
		if err != nil {
			fmt.Println(err)
		}
		fileContent := string(rawFileContent)
		var parts []string
		partSize := len(fileContent) / nReduce
		fileName := strings.Split(file, ".")[0]
		for i := 0; i < nReduce; i++ {
			id := strconv.Itoa(i)
			c.partitionedFiles[fileName+id] = 0
			start := i * partSize
			end := start + partSize
			if i == nReduce-1 {
				end = len(fileContent)
			}
			parts = append(parts, fileContent[start:end])
			os.Create("../maps/" + fileName + id + ".txt")
			os.WriteFile("../maps/"+fileName+id+".txt", []byte(parts[i]), 0644)
		}
	}

	// Your code here.

	c.server()
	return &c
}
