package mr

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Coordinator struct {
	// Pool of registered workers alongside their status
	workers map[int]string

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
	StartTime int64
	Status    int
	WorkerID  int
}

// ========= RPC Handlers ==========
func (c *Coordinator) RequestJob(workerID *int, reply *Job) error {
	log.Infof("Job request from Worker %d", *workerID)
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Info("Checking for PENDING map jobs...")
	// Check for PENDING Map Jobs and assign them
	for file, jobStatus := range c.mapJobs {
		if jobStatus.Status == PENDING {

			log.Infof("Assigned map job to Worker %d\n", *workerID)
			// Assign Job
			reply.ID = int(time.Now().UnixNano())
			reply.Action = Work
			reply.InputFile = file
			reply.Type = MapJob
			reply.WorkerID = *workerID
			reply.NReducer = c.nReduce

			// Update JobState for the corresponding file
			c.mapJobs[file] = JobState{
				Status:    RUNNING,
				StartTime: time.Now().Unix(),
				WorkerID:  *workerID,
			}

			return nil
		}
	}

	// Do not assign any reduce job until all map jobs are COMPLETED
	if !c.canReduce {
		reply.Action = Wait
		return nil
	}

	log.Info("Checking for PENDING reduce jobs...")
	// Check for PENDING reduce jobs and assign them
	for k, jobStatus := range c.reduceJobs {
		if jobStatus.Status == PENDING {

			log.Infof("Assigned reduce job to Worker %d\n", *workerID)

			// Assign Job
			reply.ID = int(time.Now().UnixNano())
			reply.Action = Work
			reply.NReducer = c.nReduce
			reply.Type = ReduceJob
			reply.ReducerBucket = k
			reply.IntermediateFiles = c.intermediateFiles[k]

			// Update JobState
			c.reduceJobs[k] = JobState{
				Status:    RUNNING,
				StartTime: time.Now().Unix(),
				WorkerID:  *workerID,
			}

			return nil
		}
	}

	return nil
}

func (c *Coordinator) SendJobResults(job *Job, reply *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch job.Type {
	case MapJob:
		log.Infof("Received map job results from Worker %d\n", job.WorkerID)
		c.mapJobs[job.InputFile] = JobState{
			Status: COMPLETED,
		}

		// Add intermediate files to corresponding bucket
		for i, file := range job.IntermediateFiles{
			c.intermediateFiles[i] = append(c.intermediateFiles[i], file)
		}

		// Check if Reduce Jobs can be assigned now
		c.canReduce = true
		for _, state := range c.mapJobs {
			c.canReduce = c.canReduce && (state.Status == COMPLETED)
			if c.canReduce {
				log.Info("Coordinator can start assigning reduce jobs")
			}
		}
	case ReduceJob:
		log.Infof("Received reduce job results from Worker %d\n", job.WorkerID)
		c.reduceJobs[job.ReducerBucket] = JobState{
			Status: COMPLETED,
		}
	default:
		panic("coordinator - got an invalid job type \n")
	}

	return nil
}

// ========= Methods ==========

// Starts a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
		if reduceJob.Status != COMPLETED {
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
		if v.Status == RUNNING {
			if time.Now().Unix() > v.StartTime+interval {
				log.Infof("Coordinator - Worker %d is unreachable, reverting JobStatus for %s\n", v.WorkerID, k)
				c.mapJobs[k] = JobState{StartTime: -1, Status: PENDING, WorkerID: 0}
				delete(c.workers, v.WorkerID)
			}
		}
	}

	for k, v := range c.reduceJobs {
		if v.Status == RUNNING {
			if time.Now().Unix() > v.StartTime+interval {
				log.Infof("Coordinator - Worker %d is unreachable, reverting JobStatus for Bucket %d\n", v.WorkerID, k)
				c.reduceJobs[k] = JobState{StartTime: -1, Status: PENDING, WorkerID: 0}
				delete(c.workers, v.WorkerID)
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
			Status:    PENDING,
			StartTime: -1,
		}
	}

	reduceJobs := make(map[int]JobState)
	for i := range nReduce {
		reduceJobs[i] = JobState{
			Status:    PENDING,
			StartTime: -1,
		}
	}

	c := &Coordinator{
		nReduce:           nReduce,
		files:             files,
		workers:           make(map[int]string),
		reduceJobs:        reduceJobs,
		mapJobs:           mapJobs,
		intermediateFiles: make(map[int][]string),
	}

	// Start checking for dead workers periodically
	c.monitor(10) // 10 Seconds

	// Start listening for workers
	c.server()

	log.Info("Coordinator started!")
	return c
}
