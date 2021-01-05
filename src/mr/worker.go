package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeToFile(f *os.File, kva *KeyValue) {
	enc := json.NewEncoder(f)
	enc.Encode(kva)
}

func doMap(id int, filename string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))

	var ofiles []*os.File
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", id, i)
		ofile, err := os.Create(oname)
		defer ofile.Close()
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		ofiles = append(ofiles, ofile)
	}

	for _, kv := range kva {
		writeToFile(ofiles[ihash(kv.Key)%nReduce], &kv)
	}
}

func readFromFile(f *os.File) []*KeyValue {
	var kva []*KeyValue
	dec := json.NewDecoder(f)
	for {
		var kv *KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func doReduce(id, nMap int, reducef func(string, []string) string) {
	var kva []*KeyValue
	for i := 0; i < nMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, id)
		ifile, err := os.Open(iname)
		defer ifile.Close()
		if err != nil {
			log.Fatalf("cannot Open %v", iname)
		}
		kva = append(kva, readFromFile(ifile)...)
	}

	k2va := make(map[string][]string)
	for _, kv := range kva {
		k2va[kv.Key] = append(k2va[kv.Key], kv.Value)
	}

	oname := fmt.Sprintf("mr-out-%d", id)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	for k, va := range k2va {
		output := reducef(k, va)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	lastTaskID := -1
	for {
		args := GetTaskArgs{
			LastTaskID: lastTaskID,
		}
		reply := GetTaskReply{}
		if !call("Master.GetTask", &args, &reply) {
			break
		}

		lastTaskID = reply.TaskID
		switch reply.TaskType {
		case TASK_TYPE_MAP:
			doMap(reply.TaskID, reply.Filename, reply.NumReduce, mapf)
		case TASK_TYPE_REDUCE:
			doReduce(reply.TaskID, reply.NumMap, reducef)
		case TASK_TYPE_FINISH:
			break
		case TASK_TYPE_IDLE:
			time.Sleep(time.Microsecond * 100)
		default:
			log.Fatalf("error task type: %v", reply.TaskType)
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Print("dialing:", err)
		return false

	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
