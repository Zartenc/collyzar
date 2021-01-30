package collyzar

import (
	"crypto/tls"
	"encoding/json"
	"github.com/go-redis/redis"
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/extensions"
	"github.com/gocolly/colly/v2/proxy"
	"github.com/gocolly/redisstorage"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type ZarResponse struct {
	StatusCode int
	Body       []byte
	Headers    *http.Header
	Request    *ZarRequest
	PushInfos  PushInfo
}

type ZarRequest struct {
	URL      *url.URL
	Headers  *http.Header
	Method   string
	ProxyURL string
}

func (zar *ZarResponse) RequestNew(newPushInfos PushInfo, isCheckHasVisited bool) error {
	ts := NewToolSpider(redisIp, redisPort, redisPW, spiderName)
	if !isCheckHasVisited{
		newPushInfos.DontFilter = true
	}
	err := ts.PushToQueue(newPushInfos)
	if err != nil {
		return err
	}
	return nil
}


type Callback func(*ZarResponse)

var gSpiderStatus = "0" //0:running 1:pause 2:stop

var (
	spiderName        string
	domain            string
	redisIp           string
	redisPort         int //default 6379
	redisPW           string
	referer           string
	cookie            string
	proxyUrls         []string
	concurrentRequest int  //default 20
	downloadTimeout   int  //default 120
	randomDelay       int  //default 0
	disableCookies    bool //fefault true
	isRetry           bool //default true
	retryTimes        int  //default 3
	isBackupCookis 	  bool //default false
	dontFilter		  bool //default false
)


func initParam(cs *CollyzarSettings, ss *SpiderSettings) {
	if cs == nil {
		log.WithFields(log.Fields{
			"collyzar": "must settings error",
		}).Fatalln("CollyzarSettings must be initialized")
		return
	}
	if ss == nil {
		ss = &SpiderSettings{}
	}
	spiderName = cs.SpiderName
	domain = cs.Domain
	redisIp = cs.RedisIp
	redisPort = cs.RedisPort //default 6379
	redisPW = cs.RedisPW

	referer = ss.Referer
	cookie = ss.Cookie
	proxyUrls = ss.ProxyUrls
	concurrentRequest = ss.ConcurrentRequest //default 20
	downloadTimeout = ss.DownloadTimeout     //default 120
	randomDelay = ss.RandomDelay             //default 0
	disableCookies = ss.DisableCookies       //default true
	isRetry = ss.IsRetry                     //default true
	retryTimes = ss.RetryTimes               //default 3
	isBackupCookis = ss.IsBackupCookis		 //default false
	dontFilter = ss.DontFilter				 //default false

	if redisPort == 0 {
		redisPort = 6379
	}
	if concurrentRequest == 0 {
		concurrentRequest = 20
	}
	if downloadTimeout == 0 {
		downloadTimeout = 120
	}
	if randomDelay == 0 {
		randomDelay = 0
	}
	if disableCookies == false {
		disableCookies = true
	}
	if isRetry == false {
		isRetry = true
	}
	if retryTimes == 0 {
		retryTimes = 3
	}

}

func Run(callback Callback, cs *CollyzarSettings, ss *SpiderSettings) {
	initParam(cs, ss)
	if spiderName == "" || domain == "" || redisIp == "" {
		log.WithFields(log.Fields{
			"collyzar": "settings error",
		}).Fatalln("must configure must-settings")

	}
	var err error
	var zarQ = &zarQueue{maxSize: concurrentRequest * 2}

	c := colly.NewCollector(
		colly.AllowedDomains(domain),
		colly.Async(true),
		//colly.AllowURLRevisit(), //if true, not check isVisited
		colly.IgnoreRobotsTxt(),
	)
	err = c.Limit(&colly.LimitRule{
		DomainGlob:  domain,
		RandomDelay: time.Duration(randomDelay) * time.Second,
		Parallelism: concurrentRequest,
	})
	if err != nil {
		log.WithFields(log.Fields{
			"collyzar": "limit rule errir",
		}).Error(err)

	}

	c.WithTransport(&http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	})

	c.SetRequestTimeout(time.Second * time.Duration(downloadTimeout))

	if disableCookies {
		c.DisableCookies()
	}

	if dontFilter{
		c.AllowURLRevisit = true
	}

	extensions.RandomUserAgent(c) //desktop ua

	if len(proxyUrls) > 0 {
		rp, err := proxy.RoundRobinProxySwitcher(proxyUrls...)
		if err != nil {
			log.WithFields(log.Fields{
				"collyzar": "set proxy switch error",
			}).Fatal(err)
		}
		c.SetProxyFunc(rp)
	}

	storage := &RedisStorage{
		Storage: &redisstorage.Storage{
			Address:  redisIp + ":" + strconv.Itoa(redisPort),
			Password: redisPW,
			DB:       1,
			Prefix:   spiderName + "_filter",
		},
		IsStorageCookies: isBackupCookis,
	}

	err = c.SetStorage(storage)
	if err != nil {
		log.WithFields(log.Fields{
			"collyzar": "set storage error",
		}).Fatal(err)
	}

	c.OnRequest(func(r *colly.Request) {
		r.Headers = handleHeaders()
	})

	c.OnError(func(r *colly.Response, err error) {
		handleError(callback, r, err)
	})

	c.OnResponse(func(r *colly.Response) {
		handleResponse(callback, r)
	})

	spiderQueue(storage.Client, c, zarQ, callback)
	c.Wait()
	cleanRedis()
}

func spiderQueue(rdb *redis.Client, c *colly.Collector, zarQ *zarQueue, callback Callback) {
	setSpiderSignal(rdb)
	go detectSpiderSignal(rdb)
	go genCache(rdb, zarQ)

	for {
		isStop := spiderWatch(c, zarQ, callback)
		if isStop {
			break
		}
		pauseTime := rand.Intn(1000) //0-1000
		time.Sleep(time.Millisecond * time.Duration(pauseTime))
	}

}

func detectSpiderSignal(rdb *redis.Client) {
	for {
		stopStatus, err := rdb.HGet("collyzar_spider_status", spiderName).Result()
		if err == redis.Nil{
			log.WithFields(log.Fields{
				"collyzar": "get redis status redis nil",
			}).Error(err)
		} else if err != nil {
			log.WithFields(log.Fields{
				"collyzar": "get redis status error",
			}).Error(err)
		}
		//pause
		if stopStatus == "1" {
			gSpiderStatus = "1"
		}
		//stop
		if stopStatus == "2" {
			gSpiderStatus = "2"
			return
		}

		time.Sleep(time.Second * 2)
	}
}

func setSpiderSignal(rdb *redis.Client) {
	_, err := rdb.HSet("collyzar_spider_status", spiderName, gSpiderStatus).Result()
	if err != nil {
		log.WithFields(log.Fields{
			"collyzar": "set spider signal error",
		}).Error(err)
	}

	_, err = rdb.Incr(spiderName + "_counts").Result()
	if err != nil {
		log.WithFields(log.Fields{
			"collyzar": "set spider count error",
		}).Error(err)
	}

}

func cleanRedis() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisIp + ":" + strconv.Itoa(redisPort),
		Password: redisPW,
		DB:       1,
	})
	defer rdb.Close()

	spidersCount := rdb.Decr(spiderName + "_counts").Val()
	if spidersCount > 0{
		return
	}

	_, err := rdb.HDel("collyzar_spider_status", spiderName).Result()
	if err == redis.Nil{
		return
	}else if err != nil {
		log.WithFields(log.Fields{
			"collyzar": "del redis bloom queue error",
		}).Error(err)
	}
	_, err = rdb.Del(spiderName + "_filter_bloom", spiderName + "_counts").Result()
	if err == redis.Nil{
		return
	} else if err != nil {
		log.WithFields(log.Fields{
			"collyzar": "del redis bloom queue error",
		}).Error(err)
	}
}

func spiderWatch(c *colly.Collector, zarQ *zarQueue, callback Callback) bool {
	if gSpiderStatus == "1" {
		log.WithFields(log.Fields{
			"collyzar": "spider status",
		}).Info("pause spider")
	} else if gSpiderStatus == "2" {
		log.WithFields(log.Fields{
			"collyzar": "stop spider",
		}).Info("Wait for spider request finish and then stop")
		return true
	}

	if gSpiderStatus != "0" {
		return false
	}

	urlInfoI := zarQ.pop()
	if urlInfoI == nil {
		return false
	}

	urlInfo := urlInfoI.(string)
	var oUrlInfo PushInfo
	err := json.Unmarshal([]byte(urlInfo), &oUrlInfo)
	if err != nil {
		log.WithFields(log.Fields{
			"collyzar": "redis push info serialize error",
		}).Error(err)
	}

	if dontFilter{
		ctx := colly.NewContext()
		ctx.Put("url_info", oUrlInfo)
		err = c.Request("GET", oUrlInfo.Url, nil, ctx, nil)
		if err != nil {
			log.WithFields(log.Fields{
				"collyzar": "visit url error",
			}).Error(err)
		}
		return false
	}

	if oUrlInfo.DontFilter{
		retryc := c.Clone()
		retryc.AllowURLRevisit = true
		retryc.OnRequest(func(r *colly.Request) {
			r.Headers = handleHeaders()
		})
		retryc.OnError(func(r *colly.Response, err error) {
			handleError(callback, r, err)
		})

		retryc.OnResponse(func(r *colly.Response) {
			handleResponse(callback, r)
		})

		ctx := colly.NewContext()
		ctx.Put("url_info", oUrlInfo)
		err = retryc.Request("GET", oUrlInfo.Url, nil, ctx, nil)
		if err != nil {
			log.WithFields(log.Fields{
				"collyzar": "user retry request error",
			}).Error(err)
		}

	}

	isV, err := c.HasVisited(oUrlInfo.Url)
	if err != nil {
		log.WithFields(log.Fields{
			"collyzar": "judge has visited error",
		}).Error(err)
	}
	if !isV {
		ctx := colly.NewContext()
		ctx.Put("url_info", oUrlInfo)
		err = c.Request("GET", oUrlInfo.Url, nil, ctx, nil)
		if err != nil {
			log.WithFields(log.Fields{
				"collyzar": "visit url error",
			}).Error(err)
		}
	}
	return false
}

func genCache(rdb *redis.Client, zarQ *zarQueue) {
	for {
		if zarQ.isFull() {
			continue
		}
		urlInfo, err := rdb.RPop(spiderName).Result()
		if err == redis.Nil{
			time.Sleep(time.Second * 1)
			continue
		}else if err != nil{
			log.WithFields(log.Fields{
				"collyzar": "get redis spider queue error",
			}).Error(err)
		}else {
			zarQ.push(urlInfo)
		}

		time.Sleep(time.Millisecond * 200)
	}
}

func handleHeaders() *http.Header{
	headers := &http.Header{}
	headers.Set("Referer", referer)
	headers.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	headers.Set("Accept-Language", "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2")
	headers.Set("Cookie", cookie)
	return headers
}

func handleResponse(callback Callback, r *colly.Response){
	statusCode := r.StatusCode
	retryHttpCodes := []int{500, 502, 503, 504, 522, 524, 408, 429}
	isRetryCode := false
	for _, s := range retryHttpCodes {
		if statusCode == s {
			isRetryCode = true
			break
		}
	}
	if isRetry && isRetryCode {
		var reqTimes int
		reqTimesIf := r.Ctx.GetAny("req_times")
		if reqTimesIf == nil {
			reqTimes = 1
			r.Request.Ctx.Put("req_times", reqTimes)
		} else {
			reqTimes = reqTimesIf.(int)
		}

		if reqTimes <= retryTimes {
			reqTimes += 1
			r.Request.Ctx.Put("req_times", reqTimes)
			err := r.Request.Retry()
			if err != nil {
				log.WithFields(log.Fields{
					"collyzar": "OnResponse retry visit Error",
				}).Error(err)
			}
		}
	} else {
		handleCallbackResponse(callback, r)
	}

}

func handleError(callback Callback, r *colly.Response, err error){
	log.WithFields(log.Fields{
		"collyzar": "problem with download",
	}).Debug(err)

	if isRetry {
		var reqTimes int
		reqTimesIf := r.Ctx.GetAny("req_times")
		if reqTimesIf == nil {
			reqTimes = 1
			r.Request.Ctx.Put("req_times", reqTimes)
		} else {
			reqTimes = reqTimesIf.(int)
		}

		if reqTimes <= retryTimes {
			reqTimes += 1
			r.Request.Ctx.Put("req_times", reqTimes)
			err = r.Request.Retry()
			if err != nil {
				log.WithFields(log.Fields{
					"collyzar": "retry visit error",
				}).Error(err)
			}
		}else {
			handleCallbackResponse(callback, r)
		}
	}else {
		handleCallbackResponse(callback, r)
	}

}

func handleCallbackResponse(callback Callback, r *colly.Response){
	zarReq := new(ZarRequest)
	zarReq.URL = r.Request.URL
	zarReq.Headers = r.Request.Headers
	zarReq.Method = r.Request.Method
	zarReq.ProxyURL = r.Request.ProxyURL

	zarRes := new(ZarResponse)
	zarRes.StatusCode = r.StatusCode
	zarRes.Body = r.Body
	zarRes.Headers = r.Headers
	zarRes.Request = zarReq
	zarRes.PushInfos = r.Ctx.GetAny("url_info").(PushInfo)
	zarRes.PushInfos.DontFilter = false

	callback(zarRes)

}