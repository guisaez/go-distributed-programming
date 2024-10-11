package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"os"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	dirPath := "./data"

	// Check if the directory already exists
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		// Directory does not exist, create it
		err := os.MkdirAll(dirPath, os.ModePerm)
		if err != nil {
			fmt.Printf("Error creating directory: %v\n", err)
			return
		}
		fmt.Println("Directory created:", dirPath)
	} else {
		// Directory already exists
		fmt.Println("Directory already exists:", dirPath)
	}
}

type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ArrKeyValue []KeyValue

// for sorting by key
func (a ArrKeyValue) Len() int           { return len(a) }
func (a ArrKeyValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ArrKeyValue) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Send an RPC request to the coordinator, wait for the response
// usually returns true
// returns false if something goes wrong
func call(rpc_name string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("Worker - error dialing: ", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpc_name, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)

	return false
}

const (
	RequestJobCall     string = "Coordinator.RequestJob"
	SendJobResultsCall string = "Coordinator.SendJobResults"
)

// ========== RPC Calls ==========

func RequestJob(PID int) *Job {
	reply := &Job{}

	if !call(RequestJobCall, &PID, reply) {
		// No Response OR Error Dialing the Coordinator will
		// make the Worker quit
		reply.Action = Quit
		return reply
	}

	return reply
}

// Send job results to the coordinator
func SendJobResults(job *Job) {
	call(SendJobResultsCall, job, &struct{}{})
}

// main/mr_worker.go calls this function
func Worker(mapFun func(string, string) []KeyValue, reduceFun func(string, []string) string) {
	workerID := os.Getpid()
	quit := false

	for !quit {
		// Request a Job From the Coordinator
		log.Infof("Requesting job for Worker %d\n", workerID)
		job := RequestJob(workerID)

		switch job.Action {
		case Wait:
			// Sleep for 10 second before asking again for a Job
			log.Infof("Worker %d waiting \n", workerID)
			time.Sleep(10 * time.Second)
		case Quit:
			quit = true
		case Work:
			// Process a MapJob
			log.Infof("Worker %d received a job \n", workerID)
			if job.Type == MapJob {
				handleMapJob(job, mapFun)
			}

			// Process a Reduce Job
			if job.Type == ReduceJob {
				handleReduceJob(job, reduceFun)
			}
		default:
			// If coordinator cannot be reached, exit
			quit = true
		}
	}
}

// ========== Methods ==========

func handleMapJob(job *Job, mapFun func(filename string, content string) []KeyValue) {
	content, err := os.ReadFile(job.InputFile)
	if err != nil {
		panic(err)
	}

	keyVals := mapFun(job.InputFile, string(content))

	sort.Sort(ArrKeyValue(keyVals))

	partitions := make([][]KeyValue, job.NReducer)

	for _, v := range keyVals {
		pKey := ihash(v.Key) % job.NReducer

		partitions[pKey] = append(partitions[pKey], v)
	}

	intermediateFiles := make([]string, job.NReducer)
	for i := range job.NReducer {
		intermediateFile := fmt.Sprintf("mr-%d-%d", job.ID, i)
		intermediateFiles[i] = intermediateFile

		log.Debug(intermediateFile)
		f, err := os.CreateTemp("./data/", intermediateFile)
		if err != nil {
			panic(err)
		}

		b, err := json.Marshal(partitions[i])
		if err != nil {
			panic(err)
		}

		f.Write(b)

		os.Rename(f.Name(), "./data/"+intermediateFile)

		f.Close()
	}

	// Add intermediate files to job state
	job.IntermediateFiles = intermediateFiles

	// Report back to the coordinator with intermediate files
	log.Infof("Worker %d sending map job results\n", job.WorkerID)
	SendJobResults(job)
}

func handleReduceJob(job *Job, f func(string, []string) string) {
	files := job.IntermediateFiles

	intermediate := []KeyValue{}

	for _, file := range files {
		content, err := os.ReadFile("./data/" + file)
		if err != nil {
			panic(err)
		}

		var in []KeyValue
		err = json.Unmarshal(content, &in)
		if err != nil {
			panic(err)
		}

		intermediate = append(intermediate, in...)
	}

	sort.Sort(ArrKeyValue(intermediate))

	out_name := fmt.Sprintf("mr-out-%d", job.ReducerBucket)
	tmpFile, err := os.CreateTemp("./data", out_name)
	if err != nil {
		panic(err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := f(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(tmpFile.Name(), "./data/"+out_name)

	log.Infof("Worker %d sending reduce job results\n", job.WorkerID)
	SendJobResults(job)
}
