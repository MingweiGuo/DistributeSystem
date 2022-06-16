package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//sort

type SortType []KeyValue

func (s SortType) Len() int {return len(s)}
func (s SortType) Less(i, j int) bool {return s[i].Key<s[j].Key}
func (s SortType) Swap(i, j int) {s[i],s[j] = s[j],s[i]}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	if err := os.MkdirAll(tmpdir,os.ModePerm); err != nil {
		log.Fatalln("MkdirAll failed, err :",err)
	}

	args := new(Args)

	for  {
		reply := new(Reply)

		call("Coordinator.Handle",args,reply)
		//fmt.Println(reply)
		switch reply.Type {
		case "map" :
			Maptask(mapf,reply)
		case "reduce":
			Reducetask(reducef,reply)
		case "wait":
			time.Sleep(time.Second)
		case "exit":
			return
		}
	}
}

func Maptask(mapf func(string, string) []KeyValue,reply *Reply)  {
	 //处理任务

	context, _ := Readfile(reply.Filename)

	mapout := mapf(reply.Filename,context)
	buckets := make([][]KeyValue,reply.Nreduce)

	//fenlei
	for _, kv := range mapout {
		reduceindex := ihash(kv.Key)%reply.Nreduce
		if buckets[reduceindex]==nil || len(buckets[reduceindex])==0{
			buckets[reduceindex] = make([]KeyValue,0)
		}
		buckets[reduceindex] = append(buckets[reduceindex],kv)
	}

	for index , bucket := range buckets {
		 		name := GetTmpFilename(index,reply.Mapindex,reply.Stamp)
				tmpname := Tmpname()
				if file , err := os.Create(tmpname); err == nil {
					enc := json.NewEncoder(file)
					for _ , kva := range bucket {
						enc.Encode(&kva)
					}
				}else {
					log.Fatalln("创建文件失败,err:",err)
				}
				if err := os.Rename(tmpname,name); err != nil {
					log.Fatalln("Rename failed,err:",err)
				}
	}
}

func Reducetask(reducef func(key string, values []string) string,reply *Reply){

	 tmpKeyvalue := make([]KeyValue,0)

	 for i := 0; i < reply.Nmap; i++ {

		 file := GetTmpFilename(reply.Reduceindex,i,reply.Stamp)

		 decvalue, err := Readjson(file)

		 if err != nil {
			 log.Fatalln("Reduce 失败，err:",err)
		 }
		 tmpKeyvalue = append(tmpKeyvalue,decvalue...)
	 }

	 sort.Sort(SortType(tmpKeyvalue))


	 outname := fmt.Sprintf("mr-out-%v",reply.Reduceindex)
	 tmpname := Tmpname()

	 outfile ,err := os.Create(tmpname)

	 if err != nil {
		 log.Fatalln(err)
	 }

	 for i := 0; i < len(tmpKeyvalue); i++ {
		  j := i + 1

		  for j < len(tmpKeyvalue) && tmpKeyvalue[i].Key == tmpKeyvalue[j].Key {
			  j++
		  }


		  values := make([]string,0)

		  for k := i ; k < j ; k++ {
			  values = append(values,tmpKeyvalue[k].Value)
		  }


		  out := reducef(tmpKeyvalue[i].Key,values)

		  fmt.Fprintf(outfile,"%v %v \n",tmpKeyvalue[i].Key,out)

		  i = j
	 }

	 if err := os.Rename(tmpname,outname); err != nil {
		 log.Fatalln("Rename failed, err :",err)
	 }

}


func Readfile(filename string) (string,error) {
	 file, err := os.Open(filename)

	 if err != nil {
		 log.Fatalf("Open file %v failed,err : %v \n",filename,err)
		 return "",err
	 }

	 context, err := ioutil.ReadAll(file)

	 if err != nil {
		 log.Fatalf("Read file %v failed,err : %v \n",filename,err)
		 return "",err
	 }


	 return string(context),nil

}


func Readjson(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil, err
	}
	intermediate := make([]KeyValue, 0)
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	return intermediate, nil
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
