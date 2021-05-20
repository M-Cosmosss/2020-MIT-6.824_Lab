package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	TaskType        int
	TaskStatus      int
	TaskIndex       int
	TaskMapIndex    int
	TaskReduceIndex int
	ReduceN         int
	FileNums        int
	FileName        string
}

const (
	MapTask    = 0
	ReduceTask = 1
	NoTask     = 3
	Ready      = 0
	Doing      = 1
	Finished   = 2
	Success    = 1
	Error      = 2
	Timeout    = time.Second * 10
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
