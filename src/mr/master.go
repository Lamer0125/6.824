package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	TaskList  []TaskStats
	NReduce   int
	NMap      int
	Phase     Phases
	Timeout   float64
	Filenames []string
	Taskch    chan Task
	IsDone bool
	mu        sync.Mutex
}

type TaskStats struct {
	State     States
	WorkerId  int
	Starttime time.Time
}

type Task struct {
	TaskType Phases
	Filename string
	WorkerId int
	NReduce int
	NMap int
	Alive bool
}

type Phases int

const (
	MapPhase    Phases = 0
	ReducePhase       = 1
)

type States int

const (
	Pending States = iota
	Queued
	Running
	Finished
	Corrupt
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) getTask(idx int) Task {
	task := Task{
		TaskType:  m.Phase,
		Filename:  "",
		WorkerId:  idx,
		NReduce: m.NReduce,
		NMap:  m.NMap,
		Alive: true,
	}
	if task.TaskType == MapPhase {
		task.Filename = m.Filenames[idx]
	}
	return task
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.IsDone {
		return
	}
	AllFinished := true
	for index, t := range m.TaskList {
		switch t.State {
		case Pending:
			m.Taskch <- m.getTask(index)
			m.TaskList[index].State = Queued
			m.TaskList[index].Starttime = time.Now()
			m.TaskList[index].WorkerId = index
			AllFinished = false
		case Queued:
			AllFinished = false
		case Corrupt:
			m.Taskch <- m.getTask(index)
			m.TaskList[index].State = Queued
			m.TaskList[index].Starttime = time.Now()
			m.TaskList[index].WorkerId = index
			AllFinished = false
		case Running:
			if time.Now().Sub(m.TaskList[index].Starttime).Seconds() >= m.Timeout{
				m.Taskch <- m.getTask(index)
				m.TaskList[index].Starttime = time.Now()
				m.TaskList[index].WorkerId = index
				AllFinished = false
			}
		}
	}
	if AllFinished {
		if m.Phase == ReducePhase {
			m.IsDone = true
			log.Print("Finished!!!")
		} else {
			log.Print("To Reduce phase")
			m.Phase = ReducePhase
			m.TaskList = make([]TaskStats, m.NReduce)
		}
		//m.taskList = make([]TaskStats, m.nReduce)
	}
}

func (m *Master) RequestTask(args *Args, reply *Reply) error {
	task := <- m.Taskch
	idx := task.WorkerId
	m.TaskList[idx].State = Running
	reply.TaskObj = task

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
func (m *Master) ReportTask(args *Args, reply *Reply) error {
	m.TaskList[args.Idx].State = args.State

	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.IsDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{}
	m.NReduce = nReduce
	m.NMap = len(files)
	m.Phase = MapPhase
	m.Timeout = 10
	m.Filenames = files
	m.TaskList = make([]TaskStats, len(files))
	if nReduce > len(files) {
		m.Taskch = make(chan Task, nReduce)
	} else {
		m.Taskch = make(chan Task, len(files))
	}
	m.server()
	log.Print("master is initializing...")
	for !m.Done() {
		go m.schedule()
		time.Sleep(time.Millisecond * 500)
	}
	// Your code here.


	return &m
}
