package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
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
type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.run()
}
func (w *worker) run() {
	// if reqTask conn fail, worker exit
	for {
		reply := w.callRequest()
		if !reply.TaskObj.Alive {
			log.Print("worker get task not alive, exit")
			return
		}
		w.doTask(reply.TaskObj)
	}
}
func (w *worker) doTask(task Task) {
	if task.TaskType == MapPhase {
		w.doMapTask(task)
	} else {
		w.doReduceTask(task)
	}
}

func (w *worker) doMapTask(task Task) {
	log.Printf("domaptask")
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Printf(".")
		log.Fatalf("cannot open %v", task.Filename)
		CallReport(Corrupt, task.WorkerId)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("...")
		log.Fatalf("cannot read %v", task.Filename)
		CallReport(Corrupt, task.WorkerId)
	}
	file.Close()
	kva := w.mapf(task.Filename, string(content))
	reduces := make([][]KeyValue, task.NReduce)
	log.Printf("ffff")
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}
	log.Printf("ddd")
	for idx, l := range reduces {
		fileName := fmt.Sprintf("mr-%d-%d", task.WorkerId, idx)
		log.Printf(fileName)
		f, err := os.Create(fileName)
		if err != nil {
			log.Printf("Opening file with error: %v", err)
			CallReport(Corrupt, task.WorkerId)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				log.Printf("///")
				CallReport(Corrupt, task.WorkerId)
			}

		}
		if err := f.Close(); err != nil {
			log.Printf("jjjj")
			CallReport(Corrupt, task.WorkerId)
		}
	}
	log.Printf("maptaskfinished")
	CallReport(Finished, task.WorkerId)
}

func (w *worker) doReduceTask(task Task) {
	log.Printf("doreducetask")
	log.Printf("%d", task.WorkerId)
	maps := make(map[string][]string)
	for idx := 0; idx < task.NMap; idx++ {
		filname := fmt.Sprintf("mr-%d-%d", idx, task.WorkerId )
		file, err := os.Open(filname)
		if err != nil {
			CallReport(Corrupt, task.WorkerId)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}

	}
	log.Printf("qqq")
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(fmt.Sprintf("mr-out-%d", task.WorkerId), []byte(strings.Join(res, "")), 0600); err != nil {
		CallReport(Corrupt, task.WorkerId)
	}
	log.Printf("reduc worker finished...")
	CallReport(Finished, task.WorkerId)
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (w *worker) callRequest() Reply{

	// declare an argument structure.
	args := Args{}

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	for {
		if call("Master.RequestTask", &args, &reply)  {
			return reply
		}
	}


}

func CallReport(state States, idx int) {
	// declare an argument structure.
	args := Args{}
	args.State = state
	args.Idx = idx

	// declare a reply structure.
	reply := Reply{}
	for {
		if call("Master.ReportTask", &args, &reply) {
			break
		}
	}

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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
