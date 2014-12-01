package nexus

import (
	"fmt"
	"time"
)

type Timing struct {
	tickers map[string]int64
	elasped map[string]int64
	keys    []string
}

func NewTiming() *Timing {
	timing := &Timing{
		tickers: make(map[string]int64),
		elasped: make(map[string]int64),
		keys:    make([]string, 0, 0),
	}
	timing.TickStart("total")
	return timing
}

func (t *Timing) TickStart(key string) {
	t.tickers[key] = time.Now().UnixNano()
	t.elasped[key] = -1
	t.keys = append(t.keys, key)
}

func (t *Timing) TickStop(key string) {
	now := time.Now().UnixNano()
	if _, ok := t.tickers[key]; !ok {
		return
	}
	t.elasped[key] = (now - t.tickers[key]) / 1000000
}

func (t *Timing) String() string {
	str := ""
	for idx, key := range t.keys {
		if t.elasped[key] == -1 {
			t.TickStop(key)
		}
		if idx != 0 {
			str = fmt.Sprintf("%s %s:%d", str, key, t.elasped[key])
		} else {
			str = fmt.Sprintf("%s:%d", key, t.elasped[key])
		}
	}
	return str
}

func (t *Timing) Finish() *map[string]int64 {
	for _, key := range t.keys {
		if t.elasped[key] == -1 {
			t.TickStop(key)
		}
	}
	return &t.elasped
}
