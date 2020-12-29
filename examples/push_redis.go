package main

import (
	"github.com/Zartenc/collyzar"
	"fmt"
)

func main(){
	ts := collyzar.NewToolSpider("127.0.0.1", 6379, "", "zarten")

	url := "https://www.amazon.com"
	mExtrainfo := make(map[string]interface{},2)
	mExtrainfo["name"] = "zarten"
	mExtrainfo["age"] = 18
	pushInfo := collyzar.PushInfo{Url:url, EInfo:collyzar.ExtraInfo{mExtrainfo}}

	err := ts.PushToQueue(pushInfo)
	if err != nil{
		fmt.Println(err)
	}

}
