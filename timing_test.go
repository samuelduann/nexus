package nexus

import (
	"fmt"
	"testing"
	"time"
)

func Test_Timing(t *testing.T) {
	timing := NewTiming()
	timing.TickStart("tick1")
	time.Sleep(100 * time.Millisecond)
	timing.TickStop("tick1")
	timing.TickStart("tick2")
	timing.TickStart("tick3")
	time.Sleep(200 * time.Millisecond)
	timing.TickStop("tick3")
	fmt.Println(timing)
	fmt.Println(*timing.Finish())
}
