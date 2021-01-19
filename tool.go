package collyzar

import (
	"encoding/json"
	"github.com/go-redis/redis"
	"strconv"
	log "github.com/sirupsen/logrus"
)

type ToolSpider struct {
	Rdb *redis.Client
	SpiderName string
}

func NewToolSpider(redisip string, redisport int, redispw ,spidername string) *ToolSpider{
	client := redis.NewClient(&redis.Options{
		Addr:     redisip + ":" + strconv.Itoa(redisport),
		Password: redispw,
		DB:       1,
	})
	_, err := client.Ping().Result()
	if err != nil{
		log.WithFields(log.Fields{
			"collyzar tool": "connect redis error",
		}).Error(err)
		return nil
	}

	return &ToolSpider{Rdb:client, SpiderName:spidername}
}

func (ts *ToolSpider)PushToQueue(pushInfo PushInfo) error{
	j, err:= json.Marshal(pushInfo)
	if err != nil{
		return err
	}

	_, err = ts.Rdb.LPush(ts.SpiderName, j).Result()
	if err != nil{
		return err
	}
	return nil
}

func (ts *ToolSpider)PauseSpiders() error{
	_, err := ts.Rdb.HSet("collyzar_spider_status", ts.SpiderName, "1").Result()
	if err != nil{
		return err
	}
	return nil
}

func (ts *ToolSpider)WakeupSpiders() error{
	_, err := ts.Rdb.HSet("collyzar_spider_status", ts.SpiderName, "0").Result()
	if err != nil{
		return err
	}
	return nil
}

func (ts *ToolSpider)StopSpiders() error{
	_, err := ts.Rdb.HSet("collyzar_spider_status", ts.SpiderName, "2").Result()
	if err != nil{
		return err
	}
	return nil
}