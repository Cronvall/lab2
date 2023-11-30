package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
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

// Worker logic for map task
func mapTaskWorker(mapf func(string, string) []KeyValue, workerID int, mapTask TaskReply) {

	//mapPath := "maps/"
	//intermediatePath := "intermediate/"

	fileName := mapTask.FileID
	nReduce := mapTask.NReduce
	jobNum := mapTask.JobNumber

	//getS3(mapPath + fileName + ".txt")

	content, err := os.ReadFile(fileName + ".txt")
	if err != nil {
		fmt.Println("Error reading file: ", err)
	}

	//Save mapping to intermediate folder
	mapped := mapf(fileName, string(content))
	sort.Sort(ByKey(mapped))

	//TODO
	//Change var. names(!!)
	dividedKv := make([][]KeyValue, nReduce)
	var kvNo int
	for _, kv := range mapped {
		kvNo = ihash(kv.Key) % nReduce
		dividedKv[kvNo] = append(dividedKv[kvNo], kv)
	}

	for i := 0; i < nReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%v-%v.json", jobNum, i)
		intermediateFile, err := os.Create(intermediateFileName)
		if err != nil {
			fmt.Println("Error saving intermediate file: ", err)
		}
		defer intermediateFile.Close()

		encoder := json.NewEncoder(intermediateFile)
		err = encoder.Encode(&dividedKv[i])
		if err != nil {
			fmt.Print("Error encoding to JSON file: ", err)
		}
		//postS3(intermediateFileName)
	}

}

func reduceTaskWorker(reducef func(string, []string) string, workerID int, reduceTask TaskReply) {

	//intermediatePath := "intermediate/"
	//fileID := reduceTask.FileID
	jobNumber := strconv.Itoa(reduceTask.JobNumber)
	//nReduce := reduceTask.NReduce

	NFiles := reduceTask.NFiles

	var interMediateKv []KeyValue
	for i := 0; i < NFiles; i++ {
		intermediateFileName := fmt.Sprintf("mr-%v-%v.json", i, jobNumber)
		//getS3(intermediateFileName)
		intermediateFile, err := os.Open(intermediateFileName)
		if err != nil {
			fmt.Println("Error opening JSON file: ", err)
		}
		defer intermediateFile.Close()
		dec := json.NewDecoder(intermediateFile)
		var kv []KeyValue
		if err := dec.Decode(&kv); err != nil {
			fmt.Println("Error decoding JSON: ", err)
		}
		interMediateKv = append(interMediateKv, kv...)

	}
	sort.Sort(ByKey(interMediateKv))
	//outputPath := "final/"

	outputFileName := "mr-out-" + jobNumber
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		fmt.Println("Error: ", err)
	}
	defer outputFile.Close()

	i := 0
	for i < len(interMediateKv) {
		j := i + 1
		for j < len(interMediateKv) && interMediateKv[j].Key == interMediateKv[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, interMediateKv[k].Value)
		}
		output := reducef(interMediateKv[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", interMediateKv[i].Key, output)
		i = j
		//fmt.Println("OUTPUT:", output)
	}
	//postS3(outputFileName)

}

func submitJob(workerID int, task TaskReply) {
	var reply SubmitTaskReply
	ok := call("Coordinator.SubmitJob", &SubmitTaskArgs{WorkerID: workerID, FileID: task.FileID, Task: task.Task}, &reply)
	if !ok || !reply.OK {
		fmt.Println("Failed Submitting job")
	}
}

func recordCompletedTask(workerID int, task TaskReply) {
	//TODO

	recordFilePath := "../records/record.txt"
	recordFile, err := os.OpenFile(recordFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening record file: ", err)
	}
	recordString := fmt.Sprintf("%d\t%s\t%s\n", workerID, task.Task, task.FileID)
	fmt.Println(recordString)
	_, err = recordFile.WriteString(recordString)
	if err != nil {
		fmt.Println("Error writing to record file: ", err)
	}
	recordFile.Close()
}

func postS3(filePath string) {
	godotenv.Load()
	region := "us-east-1"

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		fmt.Println("Error creating session:", err)
	}
	svc := s3.New(sess)

	bucket := "mapreduce-tda596"

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error opening file:", err)
		return
	}
	defer file.Close()

	// Read the contents of the file into a buffer
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, file); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading file:", err)
		return
	}

	// This uploads the contents of the buffer to S3
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filePath),
		Body:   bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		fmt.Println("Error uploading file:", err)
		return
	}

	fmt.Println("File uploaded successfully!!!")

}

func getS3(readPath string) {
	godotenv.Load()
	region := "us-east-1"

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		fmt.Println("Error creating session:", err)
	}
	svc := s3.New(sess)

	bucket := "mapreduce-tda596"

	file, err := os.Create(readPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error opening file:", err)
		return
	}
	defer file.Close()

	resp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(readPath),
	})
	if err != nil {
		fmt.Println("Error reading file from S3")
	}

	defer resp.Body.Close()
	// Write the downloaded content to the file
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		fmt.Println("Error copying the file locally")
	}

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//TODO:
	//Implement if check that checks wether the task is map or reduce
	//As of now, no logic helps split the behaviour depending on the task

	workerID := rand.Intn(9000) + 1000
	for {
		var task TaskReply
		ok := call("Coordinator.RequestTask", &TaskArgs{WorkerID: workerID}, &task)
		fmt.Println("OK: ", ok)
		if !ok {
			fmt.Println("Error calling for task")
			fmt.Println(task.FileID)
			os.Exit(4)
		}

		if !ok {
			os.Exit(4)
		}

		timerChan := make(chan bool, 1)
		taskCompletedChan := make(chan bool, 1)

		go func() {
			time.Sleep(10 * time.Second)
			timerChan <- true
		}()

		go func() {
			//time.Sleep(11 * time.Second)
			switch task.Task {
			case "map":
				mapTaskWorker(mapf, workerID, task)
				//recordCompletedTask(workerID, task)
				submitJob(workerID, task)
			case "reduce":
				fmt.Println("REDUCE")
				reduceTaskWorker(reducef, workerID, task)
				submitJob(workerID, task)
				//recordCompletedTask(workerID, task)
				//reduceTaskWorker(reducef, mapTask)
			default:
				fmt.Println("NO TASK GIVEN -> SLEEPING")
				time.Sleep(3 * time.Second)

			}
			taskCompletedChan <- true
		}()

		select {
		case <-timerChan:
			killWorker(task)
		case <-taskCompletedChan:
			fmt.Println("Task completed")

		}

	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func killWorker(task TaskReply) {
	fmt.Println("Worker timed out")
	ok := call("Coordinator.KillWorker", &KillWorker{FileID: task.FileID, Task: task.Task}, &KillWorkerReply{})
	fmt.Println("OK: ", ok)
	if !ok {
		fmt.Println("Error killing worker")
	}
	os.Exit(4)
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
	//c, err := rpc.DialHTTP("tcp", "54.242.22.243"+":1234")
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
