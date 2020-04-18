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
)

/*

When the workers and master have finished, look at the output in mr-out-*.
When you've completed the lab, the sorted union of the output files should match the sequential output, like this:




*/
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

	// uncomment to send the Example RPC to the master.
	//CallExample()
	filename := RequestTask()

	// read each input file,
	// pass it to Map,
	intermediatefile := CallMap(mapf, filename)

	CallReduce(intermediatefile, reducef)

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
func RequestTask() string {

	// declare an argument structure.
	args := RequestTaskArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := RequestTaskReply{}

	// send the RPC request, wait for the reply.
	call("Master.RequestTask", &args, &reply)

	fmt.Printf("reply.FileName %s\n", reply.FileName)
	return reply.FileName
}

//
func CallMap(mapf func(string, string) []KeyValue, filename string) string {
	DPrintf("[CallMap@worker.go] CallMap Entry")
	file, err := os.Open("../" + filename)
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

	mrfile, err := os.Create("mr.json")
	if err != nil {
		log.Fatalf("cannot open mr.json")
	}
	json.NewEncoder(file)

	enc := json.NewEncoder(mrfile)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot Encode  mr.json")
		}
	}

	DPrintf("[CallMap@worker.go] CallMap Exit")
	return "mr.json"
}

func CallReduce(filename string, reducef func(string, []string) string) {
	DPrintf("[CallReduce@worker.go] CallReduce Entry")

	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	//file, err := os.Open("./" + filename)
	if err != nil {
		log.Fatalf("cannot open [%v]", filename)
	}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	DPrintf("[CallReduce@worker.go] CallReduce Exit")
}
