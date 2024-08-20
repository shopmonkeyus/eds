package util

import (
	"context"
	"sync"
	"time"
)

type Cache interface {
	// Get a value from the cache and return true if found, any is the value if found and nil if no error
	Get(key string) (bool, any, error)

	// Set a value into the cache with a cache expiration
	Set(key string, val any, expires time.Duration) error

	// Close will shutdown the cache
	Close() error
}

type value struct {
	object  any
	expires time.Time
}

type inMemoryCache struct {
	ctx         context.Context
	cancel      context.CancelFunc
	cache       map[string]*value
	mutex       sync.RWMutex
	waitGroup   sync.WaitGroup
	once        sync.Once
	expiryCheck time.Duration
}

var _ Cache = (*inMemoryCache)(nil)

func (c *inMemoryCache) Get(key string) (bool, any, error) {
	c.mutex.RLock()
	val, ok := c.cache[key]
	c.mutex.RUnlock()
	if ok {
		if val.expires.Before(time.Now()) {
			c.mutex.Lock()
			delete(c.cache, key)
			c.mutex.Unlock()
			return false, nil, nil
		}
		return true, val.object, nil
	}
	return false, nil, nil
}

func (c *inMemoryCache) Set(key string, val any, expires time.Duration) error {
	c.mutex.Lock()
	c.cache[key] = &value{val, time.Now().Add(expires)}
	c.mutex.Unlock()
	return nil
}

func (c *inMemoryCache) Close() error {
	c.once.Do(func() {
		c.cancel()
		c.waitGroup.Wait()
	})
	return nil
}

func (c *inMemoryCache) run() {
	c.waitGroup.Add(1)
	timer := time.NewTicker(c.expiryCheck)
	defer func() {
		timer.Stop()
		c.waitGroup.Done()
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
			now := time.Now()
			c.mutex.Lock()
			var expired []string
			for key, val := range c.cache {
				if val.expires.Before(now) {
					expired = append(expired, key)
				}
			}
			if len(expired) > 0 {
				for _, key := range expired {
					delete(c.cache, key)
				}
			}
			c.mutex.Unlock()
		}
	}
}

// NewCache returns a new Cache implementation
func NewCache(parent context.Context, expiryCheck time.Duration) Cache {
	ctx, cancel := context.WithCancel(parent)
	c := &inMemoryCache{
		ctx:         ctx,
		cancel:      cancel,
		cache:       make(map[string]*value),
		expiryCheck: expiryCheck,
	}
	go c.run()
	return c
}
