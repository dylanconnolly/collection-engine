package main

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/dylanconnolly/collection-engine/engine"

	"gopkg.in/yaml.v3"
)

func main() {
	var cfg engine.Config
	readConfig(&cfg)
	validateConfig(&cfg)
	log.Printf("starting Collection-Engine")

	ce := engine.NewCollectionEngine(&cfg)
	cancel := make(chan bool)
	ce.Run(cancel)
	var wg sync.WaitGroup
	wg.Add(1)
	for {
		wg.Wait()
	}
}

func readConfig(cfg *engine.Config) {
	f, err := os.Open("helm/config.yaml")
	if err != nil {
		log.Fatalf("error reading config from 'config.yaml': %s", err)
	}
	defer f.Close()
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		log.Fatalf("error decoding config: %s", err)
	}
}

func validateConfig(cfg *engine.Config) {
	if cfg.DefaultClientTimeout == 0 {
		cfg.DefaultClientTimeout = 5 * time.Second
	}
	if cfg.DefaultWorkersCount == 0 {
		cfg.DefaultWorkersCount = 3
	}
	if cfg.SourceApi.URL == "" {
		log.Fatal("Must set Source API base url in config.yaml")
	}
	if cfg.SourceApi.AuthToken == "" {
		log.Fatal("Must set Source API auth token in config.yaml")
	}
	if cfg.SourceApi.ClientTimeout == 0 {
		cfg.SourceApi.ClientTimeout = cfg.DefaultClientTimeout
	}
	if cfg.SourceApi.RateLimitDuration == 0 {
		cfg.SourceApi.RateLimitDuration = 1
	}
	if cfg.ProcessingApi.URL == "" {
		log.Fatal("Must set Processing API base url in config.yaml")
	}
	if cfg.ProcessingApi.Timeout == 0 {
		cfg.ProcessingApi.Timeout = cfg.DefaultClientTimeout
	}
	if cfg.ProcessingApi.WorkersCount == 0 {
		cfg.ProcessingApi.WorkersCount = cfg.DefaultWorkersCount
	}
	if cfg.StorageApi.URL == "" {
		log.Fatal("Must set Storage API base url in config.yaml")
	}
	if cfg.StorageApi.Timeout == 0 {
		cfg.StorageApi.Timeout = cfg.DefaultClientTimeout
	}
	if cfg.StorageApi.WorkersCount == 0 {
		cfg.StorageApi.WorkersCount = cfg.DefaultWorkersCount
	}
}
