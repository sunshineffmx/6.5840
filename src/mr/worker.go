package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "strconv"
import "sort"
import "encoding/json"
import "io/ioutil"
import "os"


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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok || reply.TaskPhase == DoneTaskPhase {
			break
		}
		if reply.TaskPhase == MapTaskPhase {
			doMapTask(mapf, &reply.AssignedTask)
		} else if reply.TaskPhase == ReduceTaskPhase {
			doReduceTask(reducef, &reply.AssignedTask)
		}
		time.Sleep(time.Second)
	}
}

func doMapTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediate := []KeyValue{}
	file, err = os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()

	// call map function
	kva := mapf(task.Filename, string(content))
	intermediate = append(intermediate, kva...)

	// hash into NReduce buckets
	buckets := make([][]KeyValue, task.NReduce)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}
	for _, kva := range intermediate {
		idx := ihash(kva.Key) % task.NReduce
		buckets[idx] = append(buckets[idx], kva)
	}

	// write into intermediate files
	for i := range buckets {
		oname := "mr-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(i)
		ofile, _ := ioutil.TempFile("", oname + "*")
		enc := json.NewEncoder(ofile)
		for _, kva := range buckets[i] {
			err := enc.Encode(&kva)
			if err != nil {
				log.Fatalf("cannot write into %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
		ofile.Close()
	}
	reportTask(task, MapTaskPhase)
}

func doReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.Id)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out" + strconv.Itoa(task.Id)
	ofile, _ := ioutil.TempFile("", oname + "*")
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{} 	// empty slice with string type
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()

	for i := 0; i < task.NMap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.Id)
		err := os.Remove(iname)
		if err != nil {
			log.Fatalf("cannot delete %v", iname)
		}
	}

	reportTask(task, ReduceTaskPhase)
}

func reportTask(task *Task, phase int) {
	args := TaskArgs{phase, task.Id, task.Seq}
	reply := TaskReply{}
	if ok := call("Coordinator.ReportTask", &args, &reply); !ok {
		fmt.Printf("call failed\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
