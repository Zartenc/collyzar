package collyzar

//must-settings
var (
	SpiderName = ""
	Domain = ""
	RedisIp = ""
	RedisPort = 6379
	RedisPW = ""
)

//spider-settings
var (
	Referer = ""
	Cookie = ""
	ConcurrentRequest = 20
	DownloadTimeout = 120
	RandomDelay = 0
	DisableCookies = true
	ProxyUrls = []string{}
	IsRetry = true
	RetryTimes = 3

)

type PushInfo struct {
	Url string
	EInfo ExtraInfo
}

type ExtraInfo struct {
	Info map[string]interface{}
}

