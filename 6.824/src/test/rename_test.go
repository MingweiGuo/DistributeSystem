package test

import (
	"6.824/mr"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
)

type Xy struct {
	X string
	Y string
}

var stamp = mr.GetUUID()

func rename(){
	 for i := 0; i < 5 ; i++ {
		 name := mr.GetTmpFilename(0,i,stamp)
		 tmpname := mr.Tmpname()
			 if file, err := os.Create(tmpname); err == nil{
				 enc := json.NewEncoder(file)
				 for j  := 0; j < 2; j++ {
					 xy := Xy{
						 strconv.Itoa(i),
						 strconv.Itoa(j),
					 }
					 enc.Encode(&xy)
					 fmt.Println(i, j, ":", xy)
				 }
			 }else {
				 log.Fatalln("xieru shibai, err:",err)
			 }
			 if err := os.Rename(tmpname,name); err != nil {
				 log.Fatalln("chong mingming shibai ,",err)
			 }
	 }
}

func read(){
	for i := 0; i < 5 ; i++ {
			filename := mr.GetTmpFilename(0,i,stamp)
			fmt.Println(filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		intermediate := make([]Xy, 0)
		dec := json.NewDecoder(file)
		for {
			var kv Xy
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
			fmt.Println(intermediate)
	}
}

func TestRename(t *testing.T) {

	rename()
	read()

	//fmt.Printf(mr.GetUUID())

}
