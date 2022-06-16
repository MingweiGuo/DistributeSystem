package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Args struct {

}

type Reply struct {

	Type string 	//map/reduce/wait/exit
	Filename string
	Nmap	int
	Nreduce int
	Mapindex 	int
	Reduceindex	int
	Stamp 		string	//分布式唯一标识

}

func GetUUID() string {
	str, err  := exec.Command("uuidgen").Output()

	if err != nil {
		log.Fatal(err)
		return ""
	}
	return string(str)
}

var tmpdir = "./tmp"

//"./tmp/stamp-mr-mapindex-reduceindex"

func GetTmpFilename(reduceindex, mapindex int,stamp string) string {

	return fmt.Sprintf("%v/%v-mr-%v-%v",tmpdir,stamp,mapindex,reduceindex)

}

func Tmpname() string {
	return fmt.Sprintf("%v/%v", tmpdir, GetUUID())
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
