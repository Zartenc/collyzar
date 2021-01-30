package main

import (
	"github.com/Zartenc/collyzar/v2"
	"fmt"
)

func main(){
	cs := &collyzar.CollyzarSettings{
		SpiderName: "zarten",
		Domain:     "www.amazon.com",
		RedisIp:    "127.0.0.1",
	}
	ss := &collyzar.SpiderSettings{
		Referer:           "https://www.amazon.com",
		RetryTimes:			5,
		DownloadTimeout:	60,
		ProxyUrls:         []string{"your proxies"},
	}

	collyzar.Run(myResponseEx, cs, ss)
}

func myResponseEx(response *collyzar.ZarResponse){
	fmt.Println(response.StatusCode)
	fmt.Println(response.PushInfos)
	fmt.Println(string(response.Body))
}