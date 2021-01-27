package main

import (
	"github.com/Zartenc/collyzar/v2"
	"fmt"
)


func main() {
	ts := collyzar.NewToolSpider("127.0.0.1", 6379, "", "zarten")

	err := ts.StopSpiders()
	if err != nil{
		fmt.Println(err)
	}
}
