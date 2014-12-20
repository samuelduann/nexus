package nexus

import (
	"encoding/json"
	"log"
	"os"
)

type GoshineConfig struct {
	Host string
	Port int
}

type Config struct {
	GoshineClusters map[string][]GoshineConfig
	CacheTimeout    int
	Host            string
	Port            int
	Database        string
	LogPrefix       string
	DebugMode       bool
}

var conf Config

func (conf *Config) Load(configFilename string) {
	log.Print("parsing config: " + configFilename)
	cf, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	confDecoder := json.NewDecoder(cf)
	if err := confDecoder.Decode(conf); err != nil {
		log.Print("parse config failed,", err)
	}
}

func GetConfig() *Config {
	return &conf
}
