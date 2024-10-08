package mr

import (
	"os"
	"strconv"
)

// ========== RPC Definitions ===========
const MapJob, ReduceJob int = 0, 1
const Pending, Running, Completed int = 0, 1, 2
const Wait, Quit, Action int = 0, 1, 2

type Job struct {
	ID                int
	Type              int
	NReducer          int
	WorkerId          int64
	StartTime         int64
	InputFile         string
	IntermediateFiles []string
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
