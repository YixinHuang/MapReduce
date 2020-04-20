package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

/*

When the workers and master have finished, look at the output in mr-out-*.
When you've completed the lab, the sorted union of the output files should match the sequential output, like this:

*/

const nReduce int = 10

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func (m *KeyValue) String() string {
	return fmt.Sprintf("KeyValue: Key:=[%s] Value:=[%s]", m.Key, m.Value)
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
	DPrintf("[Worker@worker.go] Worker Entry")
	// Your worker implementation here.

	reqtask := Task{}
	reqtask.Reset()

	for {
		replytask := RequestTask(reqtask)

		if replytask.IsFinished() {
			DPrintf("[Worker@worker.go] Worker Exit, because Master in FinishedPhase")
			break
		}

		DPrintf("[Worker@worker.go] replytask:=[%s]", replytask.String())

		if replytask.Type == Map && replytask.Num != -1 {
			reqtask = CallMap(mapf, replytask)
			DPrintf("[Worker@worker.go] CallMap return reqtask:=[%s]", reqtask.String())
		}

		if replytask.Type == Reduce {
			reqtask = CallReduce(replytask, reducef)
			DPrintf("[Worker@worker.go] CallReduce return reqtask:=[%s]", reqtask.String())
		}

		time.Sleep(5 * time.Millisecond)
	}

	DPrintf("[Worker@worker.go] Worker Exit")
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
	DPrintf("[call@worker.go] call Entry,rpcname:=[%s]", rpcname)
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		DPrintf("[call@worker.go] call Exit.return true,rpcname:=[%s]", rpcname)
		return true
	}

	fmt.Println(err)
	DPrintf("[call@worker.go] call Exit,retur false,rpcname:=[%s]", rpcname)
	return false
}

//////////////////Implement////////////////////////////////

//RequestTask   .
//Hints 1:One way to get started is to modify mr/worker.go's Worker() to send an RPC to the master asking for a task.
//	      Then modify the master to respond with the file name of an as-yet-unstarted map task.
//		  Then modify the worker to read that file and call the application Map function, as in mrsequential.go.
func RequestTask(task Task) Task {

	// declare an argument structure.
	args := RequestTaskArgs{}

	// fill in the argument(s).

	args.WorkerPID = os.Getpid()
	args.ReqTask = task

	// declare a reply structure.
	reply := RequestTaskReply{}

	// send the RPC request, wait for the reply.
	call("Master.RequestTask", &args, &reply)

	return reply.ReplyTask
}

//
func CallMap(mapf func(string, string) []KeyValue, maptask Task) Task {
	DPrintf("[CallMap@worker.go] CallMap Entry, mapTask:=[%s]", maptask.String())

	if maptask.Num == -1 {
		DPrintf("[CallMap@worker.go] CallMap Exit, mapTask:=[%s]", maptask.String())
		return maptask
	}

	filename := maptask.FName
	mapNo := maptask.Num

	//file, err := os.Open("../" + filename)
	file, err := os.Open(filename)
	//file, err := os.Open("./" + filename)
	if err != nil {
		log.Fatalf("cannot open [%v]", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read [%v]", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	//Hints 2:A reasonable naming convention for intermediate files is mr-X-Y,
	//		  where X is the Map task number, and Y is the reduce task number.
	mapfile := make(map[int]*os.File)
	encjsn := make(map[int]*json.Encoder)

	for i := 0; i < nReduce; i++ {
		tmp := fmt.Sprintf("mr-%d-%d", mapNo, i)
		//DPrintf("[CallMap@worker.go] mapfile := [%s]", tmp)
		mapfile[i], err = os.Create(tmp)
		if err != nil {
			log.Fatalf("cannot Create file : [%s]", tmp)
		}
		json.NewEncoder(mapfile[i])
		encjsn[i] = json.NewEncoder(mapfile[i])
		defer mapfile[i].Close()
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % 10
		err := encjsn[r].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot Encode kv:=[%s]", kv.String())
		}
	}
	DPrintf("[CallMap@worker.go] CallMap Exit")
	maptask.Status = Completed
	return maptask
}

func CallReduce(reducetask Task, reducef func(string, []string) string) Task {
	DPrintf("[CallReduce@worker.go] CallReduce Entry, reducetask:=[%s]", reducetask.String())

	oname := fmt.Sprintf("mr-out-%d", reducetask.Num)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot Create [%s]", oname)
	}
	defer ofile.Close()

	intermediate := []KeyValue{}

	filename := reducetask.FName

	//TODO(hyx):need change fix number 8
	for i := 0; i < 8; i++ {

		filename = fmt.Sprintf("mr-%d-%d", i, reducetask.Num)
		//DPrintf("[CallReduce@worker.go] filename := [%s]", filename)

		file, err := os.Open(filename)
		//file, err := os.Open("./" + filename)
		if err != nil {
			DPrintf("[CallReduce@worker.go] cannot open filename := [%s]", filename)
			//log.Fatalf("cannot open [%v]", filename)
			continue
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

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	DPrintf("[CallReduce@worker.go] intermediate len := [%d] ", len(intermediate))
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		r := ihash(intermediate[i].Key) % 10
		r = r + 1 - 1
		//DPrintf("[CallReduce@worker.go]   i := [%d]   j := [%d] r := [%d] intermediate[i].Key :=[%v] ", i, j, r, intermediate[i].Key)
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	DPrintf("[CallReduce@worker.go] CallReduce Exit")
	reducetask.Status = Completed
	return reducetask
}
