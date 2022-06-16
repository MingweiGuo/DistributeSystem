package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	 sync.Mutex

	 filenames []string
	 nreduce int // n Reduce
	 timetask map[string] time.Time //The task name

	 state 	bool
	 stamp string
}

// Your code here -- RPC handlers for the worker to call.
// 不执行任务操作，只给worker具体任务，由reply返回
func (c *Coordinator)Handle(args *struct{},reply *Reply)  error{
	c.Lock()
	defer c.Unlock()

	//分配任务

	if c.handlemap(reply); reply.Type == "" {
		if c.handlereduce(reply); reply.Type == "" {
				reply.Type = "exit"
				//可能 worker 还没退出
				//coordinator 先退出了
				//time.Sleep(5*time.Second)
				c.state = true
		}
	}

	key := fmt.Sprintf("%v-%v-%v", reply.Mapindex, reply.Reduceindex, reply.Type)
	c.timetask[key] = time.Now().Add(time.Second * 10)
	reply.Stamp = c.stamp
	return nil


}

//分配任务
//重复遍历
func (c *Coordinator)handlemap(reply *Reply){
	 done := true
	 for index, name := range c.filenames {
		 //不存在或者超时未分配
		 if timebegin, ok := c.timetask[fmt.Sprintf("%v-0-%v",index,"map")];!ok ||(time.Now().After(timebegin)&& !c.judgemap(index)){
			 reply.Type = "map"
			 reply.Filename = name
			 reply.Nmap = len(c.filenames)
			 reply.Nreduce = c.nreduce
			 reply.Mapindex = index
			 return
		 }
		 //map任务不存在
		 if !c.judgemap(index){
			 done = false
		 }
	 }

	 if !done {
		 reply.Type = "wait"
	 }
}

//分配reduce
func (c *Coordinator)handlereduce(reply *Reply){

	done := true
	for i := 0; i < c.nreduce; i++ {
		timebegin, ok := c.timetask[fmt.Sprintf("0-%v-%v", i, "reduce")]
		_, err := os.Stat(fmt.Sprintf("mr-out-%v", i))
		if !ok || (time.Now().After(timebegin) && err != nil && os.IsNotExist(err)) {
			reply.Nmap = len(c.filenames)
			reply.Nreduce = c.nreduce
			reply.Reduceindex = i
			reply.Type = "reduce"
			return
		}
		if err != nil && os.IsNotExist(err) {
			done = false
		}
	}
	if !done {
		reply.Type = "wait"
	}


}



func (c *Coordinator) judgemap(index int) bool{
	 //判断是否所有的map工作都完成了
	 for i := 0 ;i< c.nreduce ;i++ {
		 name := GetTmpFilename(i,index,c.stamp)
		 if _, err := os.Stat(name); err != nil && os.IsNotExist(err) {
			 return false
		 }
	 }
	 return true

}



//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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


	// Your code here.
	return c.state
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		filenames: files,
		nreduce: nReduce,
		timetask: make(map[string]time.Time,0),
		state: false,
		stamp: GetUUID(),
	}
	// Your code here.
	//fmt.Println(files)
	c.server()
	return &c
}
