package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

// TaskPhase
const (
	MapTaskPhase = 0
	ReduceTaskPhase = 1
	DoneTaskPhase = 2
)

// TaskStatus
const (
	TaskNotAllocated = 0
	TaskRunning = 1
	TaskCompleted = 2
)

type Task struct {
	Id int
	Seq int
	Filename string
	NReduce int		// for map task
	NMap int		// for reduce task
	Status int
}

type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int
	nMap int
	taskPhase int
	tasks []Task
	taskCh chan Task
	reportCh chan bool
	seqCounter uint64
	nCompleted int
	doneCh chan bool
	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.TaskPhase = c.taskPhase
	if c.taskPhase == DoneTaskPhase {
		return nil
	}
	reply.AssignedTask = <-c.taskCh
	c.tasks[reply.AssignedTask.Id].Status = TaskRunning
	c.tasks[reply.AssignedTask.Id].Seq = c.seqCounter
	c.seqCounter++

	go func(taskId int) {
		time.Sleep(time.Duration(10) * time.Second)
		isTimeout := false
		c.mutex.Lock()
		if (c.tasks[taskId].Status != TaskCompleted) {   // timeout
			c.tasks[taskId].Status = TaskNotAllocated
			isTimeout = true
		}
		c.mutex.Unlock()
		if isTimeout {
			c.taskCh <- c.tasks[taskId]
		}
	}(reply.AssignedTask.Id)
	return nil
}

func (c *Coordinator) ReportTask(args *TaskArgs, reply *TaskReply) error {
	completed := false

	c.mutex.Lock()
	if args.TaskPhase == c.taskPhase && 
		c.tasks[args.TaskId].Status == TaskRunning &&
		c.tasks[args.TaskId].Seq == args.TaskSeq {
		c.tasks[args.TaskId].Status = TaskCompleted
		completed = true
	}
	c.mutex.Unlock()

	if completed {
		c.reportCh <- true
	}
	return nil
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	for {
		<-c.reportCh
		if c.nCompleted++; c.nCompleted == c.nMap {
			break
		}
	}
	c.mutex.Lock()
	c.phase = MapTaskPhase
	c.nCompleted = 0
	c.seqCounter = 0
	c.mutex.Unlock()
	c.initReducePhase()
	for {
		<-c.reportCh
		if c.nCompleted++; c.nCompleted == c.nReduce {
			break
		}
	}
	c.mutex.Lock()
	c.phase = DoneTaskPhase
	c.mutex.Unlock()
	doneCh <- true
}

func (c *Coordinator) initMapPhase() {
	for i, file := range files {
		c.tasks[i] := Task {
			Id: i
			Filename: file
			NReduce: c.nReduce
			Status: TaskNotAllocated
		}
		c.taskCh <- c.tasks[i]
	}
}

func (c *Coordinator) initReducePhase() {
	c.tasks[i] = make([]Task, c.nReduce)
	c.taskCh = make(chan Task, c.nReduce)
	for i = 0; i < c.nReduce; i++ {
		c.tasks[i] := Task {
			Id: i
			NMap: c.nMap
			Status: TaskNotAllocated
		}
		c.taskCh <- c.tasks[i]
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = <-doneCh

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.taskPhase = MapTaskPhase
	c.tasks = make([]Task, len(files))
	c.taskCh = make(chan Task, len(files))
	c.seqCounter = 0
	c.nCompleted = 0
	go c.schedule()

	c.server()
	return &c
}
