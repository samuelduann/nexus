package nexus

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const TIME_SLOT_NUM = 3600   // max timeout is 3600 seconds
const TIME_GRAIN_FACTOR = 10 //purge schedule grain, 1000ms / 10 = 100ms

type L1Cache struct {
	cache     map[string]interface{}
	timeSlots [TIME_SLOT_NUM * TIME_GRAIN_FACTOR][]string
	slotNum   int
	position  int
	lock      sync.Mutex
}

func NewL1Cache() *L1Cache {
	c := &L1Cache{
		cache:    make(map[string]interface{}),
		position: 0,
		slotNum:  TIME_SLOT_NUM * TIME_GRAIN_FACTOR,
	}
	go c.purgeRoutine()
	return c
}

func (c *L1Cache) getTimeoutPos(timeout int) int {
	return (c.position + timeout*TIME_GRAIN_FACTOR) % c.slotNum
}

func (c *L1Cache) Set(key string, value interface{}, timeout int) error {
	if timeout == 0 {
		return errors.New("timeout should not be 0")
	}
	if timeout > TIME_SLOT_NUM {
		return errors.New(
			fmt.Sprintf("timeout should not be longer than %d secs", TIME_SLOT_NUM))
	}

	c.lock.Lock()
	if _, ok := c.cache[key]; ok {
		return errors.New("duplicated cache entry")
	}
	c.cache[key] = value
	timeoutPos := c.getTimeoutPos(timeout)
	c.timeSlots[timeoutPos] = append(c.timeSlots[timeoutPos], key)
	c.lock.Unlock()

	return nil
}

func (c *L1Cache) Get(key string) (interface{}, bool) {
	c.lock.Lock()
	ret, ok := c.cache[key]
	c.lock.Unlock()
	return ret, ok
}

func (c *L1Cache) purgeRoutine() {
	ticker := time.Tick(1000 * time.Millisecond / TIME_GRAIN_FACTOR)
	for {
		<-ticker
		c.position = (c.position + 1) % c.slotNum
		c.lock.Lock()
		for _, key := range c.timeSlots[c.position] {
			delete(c.cache, key)
		}
		c.timeSlots[c.position] = make([]string, 0)
		c.lock.Unlock()
	}
}
