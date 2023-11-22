package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {

		//TODO:
		//Implement if check that checks wether the task is map or reduce
		//As of now, no logic helps split the behaviour depending on the task

		mapPath := "../maps/"
		intermediatePath := "../intermediate/"

		workerID := rand.Intn(9000) + 1000

		var mapTask MapTaskReply
		ok := call("Coordinator.RequestMapTask", &MapTaskArgs{WorkerID: workerID}, &mapTask)
		fmt.Println("OK: ", ok)
		if !ok || mapTask.FileID == "" {
			fmt.Println("Error")
			return
		}

		fileName := mapTask.FileID
		nReduce := mapTask.NReduce

		content, err := os.ReadFile(mapPath + fileName + ".txt")
		if err != nil {
			fmt.Println("Error reading file: ", err)
		}

		//Save mapping to intermediate folder
		mapped := mapf(fileName, string(content))
		sort.Sort(ByKey(mapped))

		intermediateFile, err := os.Create(intermediatePath + fileName + ".json")
		if err != nil {
			fmt.Println("Error saving intermediate file: ", err)
		}
		defer intermediateFile.Close()

		//TODO
		//Change var. names(!!)
		dividedKv := make([][]KeyValue, nReduce)
		var kvNo int
		for _, kv := range mapped {
			kvNo = ihash(kv.Key) % nReduce
			dividedKv[kvNo] = append(dividedKv[kvNo], kv)
		}

		encoder := json.NewEncoder(intermediateFile)
		for _, kv := range dividedKv {
			err = encoder.Encode(&kv)
			if err != nil {
				panic(err)
			}
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
