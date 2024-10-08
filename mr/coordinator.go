package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Pool of registered workers alongside their status
	workers map[int64]string

	// File names to be processed
	files []string

	// Number of reducers
	nReduce int

	// Identifies if the coordinator could start dispatching reduce jobs
	canReduce bool

	// Map Jobs
	mapJobs map[string]JobState

	// Reduce Jobs
	reduceJobs map[int]JobState

	// intermediate Files
	intermediateFiles map[int][]string

	// mutex
	mu sync.Mutex
}

type JobState struct {
	InputFile string
	StartTime int64
	Status    int
}

// ========= RPC Handlers ==========

// ========= Methods ==========

// Starts a thread that listens for RPCs from worker.go
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

// Function periodically executed by main/mr_coordinator.go to check
// if the entire job has finished
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, reduceJob := range c.reduceJobs {
		if reduceJob.Status != Completed {
			return false
		}
	}

	return true
}

// Monitor all workers to see if any job is stale and reset it.
func (c *Coordinator) monitor(interval int64) {
	ticker := time.NewTicker(time.Duration(interval) * time.Second)

	go func() {
		for {
			<-ticker.C
			if c.Done() {
				return
			}
			c.checkWorker(interval)
		}
	}()

}

func (c *Coordinator) checkWorker(interval int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.mapJobs {
		if v.Status == Running {
			if time.Now().Unix() > v.StartTime+interval {
				c.mapJobs[k] = JobState{StartTime: -1, Status: Pending}
			}
		}
	}

	for k, v := range c.reduceJobs {
		if v.Status == Running {
			if time.Now().Unix() > v.StartTime+interval {
				c.reduceJobs[k] = JobState{StartTime: -1, Status: Pending}
			}
		}
	}
}

// Creates a coordinator.
// Function called by main/mr_coordinator
// Args:
//   - files: files to analyze
//   - nReduce: number of reduce tasks to use
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	mapJobs := make(map[string]JobState)
	for _, file := range files {
		mapJobs[file] = JobState{
			Status:    Pending,
			StartTime: -1,
		}
	}

	reduceJobs := make(map[int]JobState)
	for i := range nReduce {
		reduceJobs[i] = JobState{
			Status:    Pending,
			StartTime: -1,
		}
	}

	c := &Coordinator{
		nReduce:           nReduce,
		files:             files,
		workers:           make(map[int64]string),
		reduceJobs:        reduceJobs,
		mapJobs:           mapJobs,
		intermediateFiles: make(map[int][]string),
	}

	// Start checking for dead workers periodically
	c.monitor(10) // 10 Seconds

	// Start listening for workers
	c.server()

	return c
}
