package mr

import (
	"os"
	"strconv"
)

// ========== RPC Definitions ===========
const (
	MapJob, ReduceJob           int = 0, 1
	PENDING, RUNNING, COMPLETED int = 0, 1, 2
	Wait, Quit, Work            int = 0, 1, 2
)

type Job struct {
	ID                int
	WorkerID          int
	Type              int
	NReducer          int
	InputFile         string
	IntermediateFiles []string
	ReducerBucket     int
	Action            int
}

// // Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr"
	s += strconv.Itoa(os.Getuid())
	return s
}
