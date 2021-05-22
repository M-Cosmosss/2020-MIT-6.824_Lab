package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"sync"
	"time"
)

type Master struct {
	Files          []string
	Tasks          map[int]*Task
	MapDone        bool
	ReduceDone     bool
	TaskMapDone    int
	TaskReduceDone int
	sync.RWMutex
}

func (m *Master) GetTask(args *Task, reply *Task) error {
	reply.TaskType = NoTask
	m.Lock()
	defer m.Unlock()
	TaskStr := "Map Task"
	Target := MapTask
	if m.MapDone {
		TaskStr = "Reduce Task"
		Target = ReduceTask
	}
	for _, j := range m.Tasks {
		if j.TaskStatus == Ready && j.TaskType == Target {
			j.TaskStatus = Doing
			*reply = *j
			go m.TaskWatch(j.TaskIndex)
			log.Printf("GetTask: %s %d", TaskStr, j.TaskIndex)
			return nil
		}
	}

	return nil
}

//TaskWatch watches task timeout.
func (m *Master) TaskWatch(i int) {
	time.Sleep(Timeout)
	m.RLock()
	if m.Tasks[i].TaskStatus == Doing {
		m.RUnlock()
		m.Lock()
		m.Tasks[i].TaskStatus = Ready
		m.Unlock()
		log.Printf("Task %d time out!", i)
		return
	}
	m.RUnlock()
	return
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//TaskDone report the finished task
func (m *Master) TaskDone(args *Task, reply *int) error {
	m.Lock()
	defer m.Unlock()
	if args.TaskStatus != Finished {
		log.Println("Call Done:Not Finished Task")
		return nil
	}
	if m.Tasks[args.TaskIndex].TaskStatus != Doing {
		log.Println("Call Done:Invalid Task(Maybe time out)")
		return nil
	}
	switch args.TaskType {
	case MapTask:
		m.Tasks[args.TaskIndex].TaskStatus = Finished
		m.TaskMapDone++
		if m.TaskMapDone == len(m.Files) {
			log.Println("[*]:Map Tasks all done!")
			m.MapDone = true
		}
	case ReduceTask:
		m.Tasks[args.TaskIndex].TaskStatus = Finished
		m.TaskReduceDone++
		if m.TaskReduceDone == args.ReduceN {
			log.Println("[*]:Reduce Tasks all done!")
			m.ReduceDone = true
		}
	default:
		log.Fatal("Call Done:Unknown Task type")
	}
	*reply = Success
	log.Printf("Call Done:Task %d finished!", args.TaskIndex)

	return nil
}

func (m *Master) Done() bool {
	m.RLock()
	defer m.RUnlock()
	if m.ReduceDone {
		cmd := exec.Command("sh", "-c", `rm ./mr-out-*-*`)
		cmd.Run()
		return true
	}
	return false
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{Files: files, Tasks: make(map[int]*Task), MapDone: false, ReduceDone: false}
	for i, j := range files {
		m.Tasks[i] = &Task{FileName: j, TaskType: MapTask,
			TaskStatus: Ready, TaskIndex: i,
			TaskMapIndex: i, ReduceN: nReduce}
		log.Printf("[Master]: Add Task type:Map Index:%d File:%s", i, j)
	}
	// Your code here.
	for i := 0; i < nReduce; i++ {
		m.Tasks[i+len(files)] = &Task{TaskIndex: i + len(files), TaskReduceIndex: i,
			ReduceN: nReduce, TaskStatus: Ready, TaskType: ReduceTask, FileNums: len(files)}
		log.Printf("[Master]: Add Task type:Reduce Index:%d ReduceIndex:%d", i+len(files), i)
	}
	m.server()
	return &m
}
