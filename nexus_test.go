package nexus

import (
	"testing"
)

func Test_Start(t *testing.T) {
	n := NewNexus("./config.json")
	n.Start()
}
