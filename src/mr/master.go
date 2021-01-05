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

// Your code here -- RPC handlers for the worker to call.
type taskRunner struct {
	taskQueue chan *GetTaskReply
	taskDone  map[int]chan struct{}
	wg        sync.WaitGroup
}

func makeTaskRunner(tasks []*GetTaskReply) *taskRunner {
	r := &taskRunner{
		taskQueue: make(chan *GetTaskReply),
		taskDone:  make(map[int]chan struct{}),
	}
	r.start(tasks)
	return r
}

func (r *taskRunner) start(tasks []*GetTaskReply) {
	r.wg.Add(len(tasks))
	for _, t := range tasks {
		go func(t *GetTaskReply) {
			defer r.wg.Done()
			done := make(chan struct{})
			r.taskDone[t.TaskID] = done
			for {
				r.taskQueue <- t
				select {
				case <-done:
					return
				case <-time.After(time.Second * 10):
					continue
				}
			}
		}(t)
	}
}

func (r *taskRunner) wait() {
	r.wg.Wait()
}

func (r *taskRunner) getTask(ID int) *GetTaskReply {
	if ID >= 0 {
		close(r.taskDone[ID])
	}
	select {
	case v, ok := <-r.taskQueue:
		if !ok {
			return taskFinish
		}
		return v
	default:
		return taskIdle
	}
}

type Master struct {
	// Your definitions here.
	runner *taskRunner
	done   chan struct{}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetTask method reply the tasks the worker will run.
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	t := m.runner.getTask(args.LastTaskID)
	*reply = *t
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	select {
	case <-m.done:
		return true
	default:
		return false
	}
}

func (m *Master) runTasks(files []string, nReduce int) {
	// Create taskRunner for map tasks
	var tasks []*GetTaskReply
	for i, f := range files {
		tasks = append(tasks, &GetTaskReply{
			TaskType:  TASK_TYPE_MAP,
			TaskID:    i,
			Filename:  f,
			NumReduce: nReduce,
		})
	}
	m.runner = makeTaskRunner(tasks)
	m.runner.wait()

	// Create taskRunner for reduce tasks
	tasks = []*GetTaskReply{}
	for i := 0; i < nReduce; i++ {
		tasks = append(tasks, &GetTaskReply{
			TaskType: TASK_TYPE_REDUCE,
			TaskID:   i,
			NumMap:   len(files),
		})
	}
	m.runner = makeTaskRunner(tasks)
	m.runner.wait()

	close(m.done)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		done: make(chan struct{}),
	}
	go m.runTasks(files, nReduce)
	m.server()
	return &m
}
