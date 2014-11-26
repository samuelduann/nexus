package nexus

import (
	"fmt"
	"testing"
)

func Test_Load_Config(t *testing.T) {
	config := GetConfig()
	config.Load("./config.json")
	fmt.Println(config)
	fmt.Println(config.GoshineList[0].Port)
}
