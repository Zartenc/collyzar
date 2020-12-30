# Collyzar

A distributed redis-based framework for colly.        

Collyzar provides a very simple configuration and tools to implement distributed crawling/scraping.       

## Features

- Simple configuration and clean API       
- Distributed crawling/scraping     
- Support redis command     
- Multi-machine load balancing    
- Support to pause or stop all crawling machines     
- Pass additional information to the crawler and get it inside the crawler and store it in the database    

## Installation

Add collyzar to your go.mod file:       
```
module github.com/x/y

go 1.14

require (
        github.com/Zartenc/collyzar latest
)
```     

## Example Usage

See [examples folder](https://github.com/Zartenc/collyzar/tree/master/examples "examples folder") for more detailed examples.    

### Crawler cluster machine

SpiderName must be unique.      

After running, it will always monitor the redis crawler queue for crawling until it receives a pause or stop signal.     
```
func main(){
	collyzar.SpiderName = "zarten"
	collyzar.Domain = "www.amazon.com"
	collyzar.RedisIp = "127.0.0.1"
	collyzar.Run(myResponse)
}

func myResponse(response *collyzar.ZarResponse){
	fmt.Println(response.StatusCode)
}
```    


### Control machine

#### Push url to redis queue

```
func main(){
	ts := collyzar.NewToolSpider("127.0.0.1", 6379, "", "zarten")

	url := "https://www.amazon.com"
	pushInfo := collyzar.PushInfo{Url:url}

	err := ts.PushToQueue(pushInfo)
	if err != nil{
		fmt.Println(err)
	}
}

```    

#### Tools

Provide tools including stop crawlers and pause crawlers.     

##### Stop all crawlers

```
func main() {
	ts := collyzar.NewToolSpider("127.0.0.1", 6379, "", "zarten")

	err := ts.StopSpiders()
	if err != nil{
		fmt.Println(err)
	}
}

```    

##### Pause  all crawlers

For all crawlers, the crawler process is idle after pausing the crawler.      
Then you can use the **WakeupSpiders** method to wake up the crawlers.     
```
func main() {
	ts := collyzar.NewToolSpider("127.0.0.1", 6379, "", "zarten")

	err := ts.PauseSpiders()
	if err != nil{
		fmt.Println(err)
	}
}

```     


## Bugs

Bugs or suggestions? Visit the [issue tracker](https://github.com/Zartenc/collyzar/issues "issue tracker")    

## Contributing

If you wish to contribute to this project, please branch and issue a pull request against master ("[GitHub Flow](https://guides.github.com/introduction/flow/ "GitHub Flow")").
