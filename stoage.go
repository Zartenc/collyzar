package collyzar

import (
	"encoding/binary"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gocolly/redisstorage"
	"log"
	"net/url"
	"github.com/Zartenc/collyzar/bloom"
	"sync"
)

type RedisStorage struct {
	*redisstorage.Storage
	IsStorageCookies bool
	bloomFilter *bloom.BloomFilter
	mu sync.RWMutex // Only used for cookie methods.
}

func (s *RedisStorage) Init() error {
	if s.Client == nil {
		s.Client = redis.NewClient(&redis.Options{
			Addr:     s.Address,
			Password: s.Password,
			DB:       s.DB,
		})
	}
	_, err := s.Client.Ping().Result()
	if err != nil {
		return fmt.Errorf("Redis connection error: %s", err.Error())
	}

	s.bloomFilter = bloom.New(s.Client, s.Prefix + "_bloom", 8*1024*1024*200)
	return err
}

func (s *RedisStorage) Visited(requestID uint64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, requestID)
	return s.bloomFilter.Add(b)
}

func (s *RedisStorage) IsVisited(requestID uint64) (bool, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, requestID)

	return s.bloomFilter.Exists(b)
}

func (s *RedisStorage) SetCookies(u *url.URL, cookies string) {
	// TODO(js) Cookie methods currently have no way to return an error.

	// We need to use a write lock to prevent a race in the db:
	// if two callers set cookies in a very small window of time,
	// it is possible to drop the new cookies from one caller
	// ('last update wins' == best avoided).
	if s.IsStorageCookies == false{
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// return s.Client.Set(s.getCookieID(u.Host), stringify(cnew), 0).Err()
	err := s.Client.Set(s.getCookieID(u.Host), cookies, 0).Err()
	if err != nil {
		// return nil
		log.Printf("SetCookies() .Set error %s", err)
		return
	}
}

func (s *RedisStorage) Cookies(u *url.URL) string{
	if s.IsStorageCookies == false{
		return ""
	}

	s.mu.RLock()
	cookiesStr, err := s.Client.Get(s.getCookieID(u.Host)).Result()
	s.mu.RUnlock()
	if err == redis.Nil {
		cookiesStr = ""
	} else if err != nil {
		// return nil, err
		log.Printf("Cookies() .Get error %s", err)
		return ""
	}
	return cookiesStr

}

func (s *RedisStorage) getCookieID(c string) string {
	return fmt.Sprintf("%s:cookie:%s", s.Prefix, c)
}






