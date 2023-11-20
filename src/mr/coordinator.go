package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
	// 1 = COMPLETED
	// 2 = IN-PROGRESS
	partitionedFiles map[int]string
	//Store the mapped file after status == 1
	intermediateFiles []string
	//Execute the reducing and store its status
	// 0 = IDLE
	// 1 = COMPLETED
	// 2 = IN-PROGRESS
	reduceStatus map[string]int
	//Id and completed tasks for a worker
	worker map[string]string
}

// Your code here -- RPC handlers for the worker to call.

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

	// Future planning:
	// Check if completed tasks are the same it was for worker 10 seconds earlier, if so,
	// worker has failed

	failBool := make(chan bool)
	failWorker := make(chan string)

	for worker := range c.worker {
		go func() {
			workerCopy := c.worker
			time.Sleep(10 * time.Second)
			if workerCopy[worker] == c.worker[worker] {
				// Bad, wont work fix, want to return what workedID fails and then terminate all prior tasks
				failBool <- true
				failWorker <- worker
			}
		}()
	}
	if <-failBool {
		failedWorker := <-failWorker
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
		partitionedFiles:  make(map[int]string),
		intermediateFiles: make([]string, 0),
		reduceStatus:      make(map[string]int),
		worker:            make(map[string]string),
	}

	for _, file := range files {
		rawFileContent, err := os.ReadFile(file)
		if err != nil {
			fmt.Println(err)
		}
		fileContent := string(rawFileContent)
		var parts []string
		partSize := len(fileContent) / nReduce
		for i := 0; i < nReduce; i++ {
			c.partitionedFiles[0] = file + "i"
			start := i * partSize
			end := start + partSize
			if i == nReduce-1 {
				end = len(fileContent)
			}
			parts = append(parts, fileContent[start:end])
			os.Create("../jobs/" + file + string(i) + ".txt")
			os.WriteFile("../jobs/"+file+string(i)+".txt", []byte(parts[i]), 0644)
		}
	}

	// Your code here.

	c.server()
	return &c
}
