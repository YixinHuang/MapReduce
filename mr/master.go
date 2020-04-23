package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	mrphase MRPhaseType

	maplist    []Task //task list
	reducelist []Task //task list
	//worker list
	mu sync.Mutex // Lock to protect shared access to this
}

/*        RPC


 */
/*------------------Paper------------------
3.1 Execution Overview
	1.The MapReduce library in the user program ﬁrst splits the input ﬁles into M pieces of typically 16 megabytes to 64 megabytes (MB) per piece
	  (controllable by the user via an optional parameter).
	  It then starts up many copies of the program on a cluster of machines.
	2.There are M map tasks and R reduce tasks to assign.
	  The master picks idle workers and assigns each one a map task or a reduce task.
	3.A worker who is assigned a map task reads the contents of the corresponding input split.
	  It parses key/value pairs out of the input data and passes each pair to the user-deﬁned Map function.
	  The intermediate key/value pairs produced by the Map function are buffered in memory.
	4.Periodically, the buffered pairs are written to local disk, partitioned into R regions by the partitioning function.
	  The locations of these buffered pairs on the local disk are passed back to the master,
	  who is responsible for forwarding these locations to the reduce workers.
	5.When a reduce worker is notiﬁed by the master about these locations,
	  it uses remote procedure calls to read the buffered data from the local disks of the map workers.
	  When a reduce worker has read all intermediate data, it sorts it by the intermediate keys so that
	  all occurrences of the same key are grouped together. The sorting is needed because typically many
	  different keys map to the same reduce task. If the amount of intermediate data is too large to ﬁt in memory, an external sort is used.
	6.The reduce worker iterates over the sorted intermediate data and for each unique intermediate key encountered,
	  it passes the key and the corresponding set of intermediate values to the user’s Reduce function.
	  The output of the Reduce function is appended to a ﬁnal output ﬁle for this reduce partition.
	7. When all map tasks and reduce tasks have been completed, the master wakes up the user program.
	  At this point, the MapReduce call in the user program returns back to the user code.


3.2 Master Data Structures
	For each map task and reduce task, it stores the state (idle, in-progress, or completed),
	and the identity of the worker machine (for non-idle tasks).
	The master is the conduit through which the location of intermediate ﬁle regions is propagated
	from map tasks to reduce tasks. Therefore, for each completed map task, the master stores the
	locations and sizes of the R intermediate ﬁle regions produced by the map task. Updates to this
	location and size information are received as map tasks are completed. The information is pushed
	incrementally to workers that have in-progress reduce tasks.
*/

/*-----------------MIT------------------
//    Job
Implement a distributed MapReduce, consisting of two programs, the master and the worker.
There will be just one master process, and one or more worker processes executing in parallel.
In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine.
The workers will talk to the master via RPC. Each worker process will ask the master for a task, read the task's input from one or more files,
execute the task, and write the task's output to one or more files.
The master should notice if a worker hasn't completed its task in a reasonable amount of time
(for this lab, use ten seconds), and give the same task to a different worker.

The test script expects to see output in files named mr-out-X
	sort mr-out* | grep . > mr-wc-all
	if cmp mr-wc-all mr-correct-wc.txt
	then
		  echo '---' wc test: PASS



//Rule 1: #The map phase should divide the intermediate keys into buckets for nReduce reduce tasks,
//		  #where nReduce is the argument that main/mrmaster.go passes to MakeMaster().
//Rule 2: #The worker implementation should put the output of the X'th reduce task in the file mr-out-X.
//Rule 3: #A mr-out-X file should contain one line per Reduce function output.
		  The line should be generated with the Go "%v %v"format, called with the key and value.
		  Have a look in main/mrsequential.go for the line commented "this is the correct format".
		  The test script will fail if your implementation deviates too much from this format.
		  		// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
//Rule 4: #The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.
//Rule 5: #main/mrmaster.go expects mr/master.go to implement a Done() method that returns true when the MapReduce job is completely finished;
		  at that point, mrmaster.go will exit.
//Rule 6: #When the job is completely finished, the worker processes should exit.
		  A simple way to implement this is to use the return value from call(): if the worker fails to contact the master,
		  it can assume that the master has exited because the job is done, and so the worker can terminate too.
		  Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the master can give to workers.


//Hints 1:#One way to get started is to modify mr/worker.go's Worker() to send an RPC to the master asking for a task.
//	      #Then modify the master to respond with the file name of an as-yet-unstarted map task.
//		  #Then modify the worker to read that file and call the application Map function, as in mrsequential.go.
//Hints 2:#A reasonable naming convention for intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.
//Hints 3:#The worker's map task code will need a way to store intermediate key/value pairs in files in a way
		  that can be correctly read back during reduce tasks. One possibility is to use Go's encoding/json package.
//Hints 4:#The map part of your worker can use the ihash(key) function (in worker.go) to pick the reduce task for a given key.
//Hints 5:You can steal some code from mrsequential.go for reading Map input files, for sorting intermedate key/value pairs between the Map and Reduce,
		  and for storing Reduce output in files.
//Hints 6:The master, as an RPC server, will be concurrent; don't forget to lock shared data.
//Hints 7:#Workers will sometimes need to wait, e.g. reduces can't start until the last map has finished.
		  #One possibility is for workers to periodically ask the master for work, sleeping with time.Sleep() between each request.
		  Another possibility is for the relevant RPC handler in the master to have a loop that waits, either with time.Sleep() or sync.Cond.
		  Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the master from processing other RPCs.
//Hints 8:The master can't reliably distinguish between crashed workers, workers that are alive but have stalled for some reason,
		  and workers that are executing but too slowly to be useful. The best you can do is have the master wait for some amount of time,
		  and then give up and re-issue the task to a different worker. For this lab, have the master wait for ten seconds;
		  after that the master should assume the worker has died (of course, it might not have).
//Hints 9:To test crash recovery, you can use the mrapps/crash.goapplication plugin. It randomly exits in the Map and Reduce functions.
//Hints10:To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions
		 the trick of using a temporary file and atomically renaming it once it is completely written.
		 You can use ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.



*/
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	DPrintf("[server@master.go] Example Entry")
	reply.Y = args.X + 1
	DPrintf("[server@master.go] Example Exit")
	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	DPrintf("[RequestTask@master.go][%d] RequestTask Entry,m.mrphase:=[%s],args.ReqTask:=[%s]", args.WorkerPID, m.mrphase.String(), args.ReqTask.String())

	m.mu.Lock()
	defer m.mu.Unlock()

	replytask := Task{}
	replytask.Reset()

	DPrintf("[RequestTask@master.go][%d]  Got Lock", args.WorkerPID)

	switch m.mrphase {
	case MapPhase:
		if args.ReqTask.Num == -1 {
			replytask = m.QueryTask()
		} else {
			if args.ReqTask.Status == Completed {
				DPrintf("[RequestTask@master.go] args.WorkerPID:=[%d]  MapPhase Update Task List Info", args.WorkerPID)
				m.UpdateTask(Map, &args.ReqTask)
				replytask = m.QueryTask()
			}
		}
	case ReducePhase:
		if args.ReqTask.Num == -1 {
			replytask = m.QueryTask()
		} else {
			if args.ReqTask.Status == Completed {
				DPrintf("[RequestTask@master.go] args.WorkerPID:=[%d]  ReducePhase Update Task List Info", args.WorkerPID)
				m.UpdateTask(Reduce, &args.ReqTask)
				replytask = m.QueryTask()
			}
		}
	case FinishPhase:
		DPrintf("[RequestTask@master.go] args.WorkerPID:=[%d]  FinishPhase return empty task", args.WorkerPID)
		replytask.FName = "FinishPhase"
	default:
		DPrintf("[RequestTask@master.go][%d]XXX Default ", args.WorkerPID)
	}

	reply.ReplyTask = replytask

	m.DumpTaksList()

	DPrintf("[RequestTask@master.go][%d] RequestTask Exit, replytask:=[%s] ,m.mrphase:=[%s]", args.WorkerPID, replytask.String(), m.mrphase.String())
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	DPrintf("[server@master.go] server Entry")
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	DPrintf("[server@master.go] sockname:=[%s]", sockname)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	DPrintf("[server@master.go] server Exit")
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if m.mrphase == FinishPhase {
		ret = true
		DPrintf("[Done@master.go]Done Exit ,ret:=[%t] because FinishPhase", ret)
	}
	//DPrintf("[Done@master.go]Done Exit ,ret:=[%t]",ret )
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	DPrintf("[MakeMaster@master.go]MakeMaster Entry ,nReduce:=[%d]", nReduce)
	m := Master{}
	m.mrphase = MapPhase

	for i, fname := range files {
		DPrintf("[MakeMaster@master.go] files[%d] := [%s]", i, fname)
		maptask := Task{
			Num:    i,    //task number
			Type:   Map,  //task cmd :map or reduce
			Status: Idle, //task state:idle, in-progress, or completed
			FName:  fname,
		}
		m.maplist = append(m.maplist, maptask)
	}

	for i := 0; i < nReduce; i++ {
		reducetask := Task{
			Num:    i,      //task number
			Type:   Reduce, //task cmd :map or reduce
			Status: Idle,   //task state:idle, in-progress, or completed
			FName:  "mr-0-0",
		}
		m.reducelist = append(m.reducelist, reducetask)
		m.DumpTaksList()
	}

	m.server()
	DPrintf("[MakeMaster@master.go]MakeMaster Exit")
	return &m
}

func (m *Master) DumpTaksList() {
	for i, task := range m.maplist {
		DPrintf("[DumpTaksList@master.go] m.maplist[%d] := [%s]", i, task.String())
	}
	for i, task := range m.reducelist {
		DPrintf("[DumpTaksList@master.go] m.reducelist[%d] := [%s]", i, task.String())
	}
}

func (m *Master) QueryTask() Task {
	DPrintf("[QueryTask@master.go] QueryTask Entry ,m.mrphase:=[%s]", m.mrphase.String())
	replytask := Task{}
	replytask.Reset()

	if m.mrphase == MapPhase {
		for i, task := range m.maplist {
			if task.Status == Idle {
				DPrintf("[QueryTask@master.go] m.maplist[%d] := [%s]", i, task.String())
				m.maplist[i].Status = InProgress
				go m.maplist[i].ResetStatus(m)
				return task
			}
		}
	}
	if m.mrphase == ReducePhase {
		for i, task := range m.reducelist {
			if task.Status == Idle {
				DPrintf("[QueryTask@master.go] m.reducelist[%d] := [%s]", i, task.String())
				m.reducelist[i].Status = InProgress
				go m.reducelist[i].ResetStatus(m)
				return task
			}
		}
	}
	if m.mrphase == FinishPhase {
		DPrintf("[QueryTask@master.go] FinishPhase Return Empty Task")
	}
	DPrintf("[QueryTask@master.go] QueryTask Exit, replytask:=[%s] m.mrphase:=[%s]", replytask.String(), m.mrphase.String())
	return replytask
}

func (m *Master) UpdateTask(tasktype TaskType, uptask *Task) {
	DPrintf("[UpdateTask@master.go] UpdateTask Entry")
	DPrintf("[UpdateTask@master.go] tasktype:=[%s] task:=[%s]", tasktype.String(), uptask.String())

	allCompleted := true

	if tasktype == Map {
		for i, task := range m.maplist {
			if task.Status != Completed {
				allCompleted = false
			}
			if task.Num == uptask.Num && uptask.Type == Map {
				m.maplist[i].Status = uptask.Status
				DPrintf("[UpdateTask@master.go] m.maplist[%d] := [%s]", i, m.maplist[i].String())
				if i == len(m.maplist)-1 && allCompleted && m.mrphase == MapPhase {
					DPrintf("[UpdateTask@master.go] *******************Change to Reduce Phase 1 allCompleted:=[%t]********************", allCompleted)
					m.mrphase = ReducePhase
				}
			}
		}
		//Doucble check
		if allCompleted && m.mrphase == MapPhase {
			DPrintf("[UpdateTask@master.go] *******************Change to Reduce Phase 2 allCompleted:=[%t]********************", allCompleted)
			m.mrphase = ReducePhase
		}

	}

	allCompleted = true

	if tasktype == Reduce {
		for i, task := range m.reducelist {
			if task.Status != Completed {
				allCompleted = false
			}
			if task.Num == uptask.Num && uptask.Type == Reduce {
				m.reducelist[i].Status = uptask.Status
				DPrintf("[UpdateTask@master.go] m.reducelist[%d] := [%s]", i, m.reducelist[i].String())
				if i == len(m.reducelist)-1 && allCompleted && m.mrphase == ReducePhase {
					DPrintf("[UpdateTask@master.go] *******************Change to Finish Phase 1 allCompleted:=[%t]********************", allCompleted)
					m.mrphase = FinishPhase
				}
			}
		}
		//Double Check
		if allCompleted && m.mrphase == ReducePhase {
			DPrintf("[UpdateTask@master.go] *******************Change to Finish PhasePhase 2 allCompleted:=[%t]********************", allCompleted)
			m.mrphase = FinishPhase
		}

	}

	DPrintf("[UpdateTask@master.go] UpdateTask Exit")

}
