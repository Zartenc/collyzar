package collyzar

//must-settings.
//Structure members must be initialized.
type CollyzarSettings struct {
	SpiderName string
	Domain string
	RedisIp string
	RedisPort int //default 6379
	RedisPW string
}

//spider-settings.
//Structure members are optional.
type SpiderSettings struct {
	Referer string
	Cookie string
	ProxyUrls []string
	ConcurrentRequest int //default 20
	DownloadTimeout int //default 120
	RandomDelay int //default 0
	DisableCookies bool //default true
	IsRetry bool //default true
	RetryTimes int //default 3
}

type PushInfo struct {
	Url string
	EInfo ExtraInfo
}

type ExtraInfo struct {
	Info map[string]interface{}
}

