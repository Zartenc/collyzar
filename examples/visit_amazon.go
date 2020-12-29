package main

import (
	"github.com/Zartenc/collyzar"
	"fmt"
)

func main(){
	collyzar.SpiderName = "zarten"
	collyzar.Domain = "www.amazon.com"
	collyzar.RedisIp = "127.0.0.1"
	collyzar.Run(myResponse)
}

func myResponse(response *collyzar.ZarResponse){
	fmt.Println(response.StatusCode)
	fmt.Println(response.PushInfos)
}