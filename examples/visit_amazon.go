package main

import (
	"github.com/Zartenc/collyzar"
	"fmt"
)

func main(){
	cs := &collyzar.CollyzarSettings{
		SpiderName: "zarten",
		Domain:     "www.amazon.com",
		RedisIp:    "127.0.0.1",
	}
	collyzar.Run(myResponse, cs, nil)
}

func myResponse(response *collyzar.ZarResponse){
	fmt.Println(response.StatusCode)
	fmt.Println(response.PushInfos)
}