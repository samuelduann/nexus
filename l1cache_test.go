package nexus

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func randInt(min int, max int) int {
	if max-min <= 0 {
		return min
	}
	rand.Seed(time.Now().UTC().UnixNano())
	return min + rand.Intn(max-min)
}

func Test_Get(t *testing.T) {
	c := NewL1Cache()

	c.Set("foo", "bar", 3)
	result, ok := c.Get("foo")
	if ok == false {
		t.Error("set but cannot get")
	}
	if result != "bar" {
		t.Error("get value not equal to set value")
	}
}

func Test_Set(t *testing.T) {
	c := NewL1Cache()
	ret := c.Set("foo", "bar", 0)
	if ret == nil {
		t.Error("should not set with timeout = 0")
	}

	ret = c.Set("foo", "bar", 86401)
	if ret == nil {
		t.Error("should return an error")
	}

	ret = c.Set("foo", "bar", 3)
	if ret != nil {
		t.Error("set failed")
	}

	ret = c.Set("foo", "bar2", 3)
	if ret == nil {
		t.Error("duplicate set should not happen")
	}

}

func Test_Purge(t *testing.T) {
	c := NewL1Cache()
	for i := 0; i < 2000; i++ {
		time.Sleep(10 * time.Millisecond)
		fmt.Println("cache size", len(c.cache))
		if i < 1000 {
			timeout := randInt(1, 10)
			c.Set(fmt.Sprintf("item%d", i), fmt.Sprintf("value%d", i), timeout)
		}
	}
	if len(c.cache) != 0 {
		t.Error("cache purge problem")
	}
}
