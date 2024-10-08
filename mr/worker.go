package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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
		log.Fatal("mr_worker - error dialing: ", err)
	}
	defer c.Close()

	err = c.Call(rpc_name, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)

	return false
}

// ========== RPC Calls ==========

func RequestJob() Job {

	workerID := os.Getpid()

	reply := Job{}

	call("Coordinator.RequestJob", &workerID, &reply)

	return reply
}

func SendJobResults(j Job) {
	call("Coordinator.SendJobResults", &j, nil)
}

// main/mr_worker.go calls this function
func Worker(mapFun func(string, string) []KeyValue, reduceFun func(string, []string) string) {

	quit := false
	for !quit {

		job := RequestJob()

		switch job.Action {
		case Wait:
			time.Sleep(10 * time.Second)
			continue
		case Quit:
			quit = true
			continue
		case Action:
			// Process job
			if job.Type == MapJob {
				handleMapJob(job, mapFun)
			}

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

func handleMapJob(j Job, mapFun func(filename string, content string) []KeyValue) {

	content, err := os.ReadFile(j.InputFile)
	if err != nil {
		log.Fatalf("worker - cannot read %v - %v \n", j.InputFile, err)
	}

	keyVals := mapFun(j.InputFile, string(content))

	sort.Sort(ArrKeyValue(keyVals))

	partions := make([][]KeyValue, j.NReducer)

	for _, v := range keyVals {
		pKey := ihash(v.Key) & j.NReducer
		partions[pKey] = append(partions[pKey], v)
	}

	intermediateFiles := make([]string, j.NReducer)
	for i := range j.NReducer {
		intermediateFile := fmt.Sprintf("mr-%v-%v", j.ID, i)
		intermediateFiles[i] = intermediateFile

		f, _ := os.Create(intermediateFile)

		b, err := json.Marshal(partions[i])
		if err != nil {
			log.Printf("worker - Marshal error: %v \n", err)
		}

		f.Write(b)

		f.Close()
	}

	// Add intermediate files to job state
	j.IntermediateFiles = intermediateFiles

	// Report back to the coordinator with intermediate files
	SendJobResults(j)
}

func handleReduceJob(j Job, f func(string, []string) string) {

}
