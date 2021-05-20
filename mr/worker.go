package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func Ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(filename string, contents string) []KeyValue,
	reducef func(string, []string) string) {
	log.Println("[Worker]: Start!")

	NoTaskTimes:=0

	for{
		task:=CallGetTask()
		switch task.TaskType {
		case MapTask:
			DoMap(mapf,task)
			log.Printf("[Worker]: Task %d finished",task.TaskIndex)
		case ReduceTask:
			DoReduce(reducef,task)
			log.Printf("[Worker]: Task %d finished",task.TaskIndex)
		case NoTask:
			log.Println("[Worker]: No Task")
			if NoTaskTimes++;NoTaskTimes==10{
				log.Fatal("Retry 10 times... Exit!")
			}
			time.Sleep(time.Second*1)
			continue
		default:
			log.Fatal("Unknown Task type")
		}
		task.TaskStatus=Finished
		if task.TaskType!=NoTask{
			CallDone(task)
			time.Sleep(time.Second)
		}

	}
}

func DoMap(mapf func(filename string, contents string) []KeyValue,task *Task)  {
	contents := OpenReadAll(task.FileName)
	kva:=mapf(task.FileName,string(contents))

	fd := make([]*os.File, task.ReduceN)
	for i, _ := range fd {
		filename := "mr-out-" + strconv.Itoa(task.TaskMapIndex) + "-" + strconv.Itoa(i)
		var err error
		//delete the tmp file which maybe exist
		os.Remove(filename)

		fd[i], err = os.Create(filename)
		Chk(err)
		defer fd[i].Close()
	}

	for _, j := range kva {
		ReduceI := Ihash(j.Key) % 10
		js, err := json.Marshal(j)
		Chk(err)
		fd[ReduceI].Write(js)
	}

}

func DoReduce(reducef func(key string, values []string) string,task *Task) {
	kva:=[]KeyValue{}
	all:=make([]byte,0)
	for i:=0;i<task.FileNums;i++{
		filename:="mr-out-"+strconv.Itoa(i)+"-"+strconv.Itoa(task.TaskReduceIndex)
		all=append(all,OpenReadAll(filename)...)
	}
	//log.Println(string(all[0:30]))
	decoder:=json.NewDecoder(bytes.NewReader(all))
	kv:=&KeyValue{}
	for decoder.Decode(kv)==nil {
		kva=append(kva,*kv)
	}

	sort.Sort(ByKey(kva))


	oname := "mr-out-"+strconv.Itoa(task.TaskReduceIndex)
	os.Remove(oname)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	return
}

func OpenReadAll(file string) []byte {
	fd, err := os.Open(file)
	defer fd.Close()
	Chk(err)
	contents, err := ioutil.ReadAll(fd)
	Chk(err)
	return contents
}

func CallGetTask() *Task{
	reply := Task{}
	call("Master.GetTask", &Task{}, &reply)

	switch reply.TaskType {
	case MapTask:
		log.Printf("[Worker]: type:Map  file: %s",reply.FileName)
	case ReduceTask:
		log.Printf("[Worker]: type:Reduce Index:%d ReduceIndex:%d",reply.TaskIndex,reply.TaskReduceIndex)
	case NoTask:
		log.Println("[Worker]: type:NoTask")
	default:
		log.Fatal("Unknown TaskType ask")
	}
	return &reply
}

func CallDone(task *Task) {
	reply:=0
	call("Master.TaskDone",task,&reply)
	switch reply{
	case Success:
		log.Println("[Worker]: Call Done Success")
	case Error:
		log.Fatal("Call Done Error")
	default:
		log.Fatal("Unknown CallDone reply")
	}
	return
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
