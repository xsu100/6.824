package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	TASK_TYPE_MAP    int = 0
	TASK_TYPE_REDUCE int = 1
	TASK_TYPE_IDLE   int = 2
	TASK_TYPE_FINISH int = 3
)

var (
	taskIdle *GetTaskReply = &GetTaskReply{
		TaskType: TASK_TYPE_IDLE,
		TaskID:   -1,
	}

	taskFinish *GetTaskReply = &GetTaskReply{
		TaskType: TASK_TYPE_FINISH,
		TaskID:   -1,
	}
)

// GetTask method args.
type GetTaskArgs struct {
	LastTaskID int
}

// GetTask method reply.
type GetTaskReply struct {
	TaskType int
	TaskID   int
	// for map task
	Filename  string
	NumReduce int
	// for reduce task
	NumMap int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
