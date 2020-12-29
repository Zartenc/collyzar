package collyzar

import (
	"encoding/json"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

type ToolSpider struct {
	Pool *redis.Pool
	SpiderName string
}

func NewToolSpider(redisIp string, redisPort int, redisPw ,spiderName string) *ToolSpider{
	pool := &redis.Pool{
		MaxActive:1000,
		MaxIdle:500,
		Wait:true,
		Dial:func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", redisIp + ":" + strconv.Itoa(redisPort),
				redis.DialPassword(redisPw),
				redis.DialDatabase(0),)
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
	}
	return &ToolSpider{Pool:pool, SpiderName:spiderName}
}

func (ts *ToolSpider)PushToQueue(pushInfo PushInfo) error{
	rdb := ts.Pool.Get()
	defer rdb.Close()

	j, err:= json.Marshal(pushInfo)
	if err != nil{
		return err
	}

	_, err = rdb.Do("LPUSH", ts.SpiderName, j)
	if err != nil{
		return err
	}
	return nil
}

func (ts *ToolSpider)PauseSpiders() error{
	rdb := ts.Pool.Get()
	defer rdb.Close()

	_, err := rdb.Do("HSET", "collyzar_spider_status", ts.SpiderName, "1")
	if err != nil{
		return err
	}
	return nil
}

func (ts *ToolSpider)WakeupSpiders() error{
	rdb := ts.Pool.Get()
	defer rdb.Close()

	_, err := rdb.Do("HSET", "collyzar_spider_status", ts.SpiderName, "0")
	if err != nil{
		return err
	}
	return nil
}

func (ts *ToolSpider)StopSpiders() error{
	rdb := ts.Pool.Get()
	defer rdb.Close()

	_, err := rdb.Do("HSET", "collyzar_spider_status", ts.SpiderName, "2")
	if err != nil{
		return err
	}

	return nil
}