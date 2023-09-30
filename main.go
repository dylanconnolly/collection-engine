package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dylanconnolly/collection-engine/engine"
	"gopkg.in/yaml.v2"
)

func main() {
	var fc FileConfig
	fc.ReadFromFile("/etc/config/config.yaml")
	var cfg engine.Config
	fc.ConvertToEngineConfig(&cfg)
	validateConfig(&cfg)
	fmt.Printf("engine config: %+v", cfg)
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

type FileConfig struct {
	DefaultClientTimeout string `yaml:"defaultClientTimeout"`
	DefaultWorkersCount  string `yaml:"defaultWorkersCount"`
	SourceApi            struct {
		URL               string `yaml:"baseUrl"`
		AuthToken         string `yaml:"authToken"`
		ClientTimeout     string `yaml:"timeout"`
		RateLimit         string `yaml:"rateLimit"`
		RateLimitDuration string `yaml:"rateLimitPeriodSecs"`
	} `yaml:"sourceApi"`
	ProcessingApi struct {
		URL          string `yaml:"baseUrl"`
		Timeout      string `yaml:"timeout"`
		WorkersCount string `yaml:"workersCount"`
	} `yaml:"processingApi"`
	StorageApi struct {
		URL          string `yaml:"baseUrl"`
		Timeout      string `yaml:"timeout"`
		WorkersCount string `yaml:"workersCount"`
	} `yaml:"storageApi"`
}

func (f *FileConfig) ReadFromFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("error reading config from '%s': %s", path, err)
	}

	err = yaml.Unmarshal(data, &f)
	if err != nil {
		log.Fatalf("error unmarshalling config data, err: %s", err)
	}
}

func (f *FileConfig) ConvertToEngineConfig(cfg *engine.Config) {
	defaultTimeout, err := time.ParseDuration(f.DefaultClientTimeout)
	if err != nil {
		log.Print("error converting default timeout value in config. Setting to default.")
		defaultTimeout = 0
	}
	cfg.DefaultClientTimeout = defaultTimeout

	val, err := strconv.Atoi(f.DefaultWorkersCount)
	if err != nil {
		log.Print("error converting default workers count config value. Setting to default")
		val = 0
	}
	cfg.DefaultWorkersCount = val

	cfg.SourceApi.URL = f.SourceApi.URL
	cfg.SourceApi.AuthToken = f.SourceApi.AuthToken
	cfg.ProcessingApi.URL = f.ProcessingApi.URL
	cfg.StorageApi.URL = f.StorageApi.URL

	timeout, err := time.ParseDuration(f.SourceApi.ClientTimeout)
	if err != nil {
		log.Printf("error converting Source API Timeout config value to time: %s", err)
		timeout = 0
	}
	cfg.SourceApi.ClientTimeout = timeout

	val, err = strconv.Atoi(f.SourceApi.RateLimit)
	if err != nil {
		log.Printf("error converting Source API Rate Limit config value to int: %s", err)
		val = 0
	}
	cfg.SourceApi.RateLimit = val

	val, err = strconv.Atoi(f.SourceApi.RateLimitDuration)
	if err != nil {
		log.Printf("error converting Source API Rate Limit Duration config value to int: %s", err)
		val = 0
	}
	cfg.SourceApi.RateLimitDuration = val

	timeout, err = time.ParseDuration(f.ProcessingApi.Timeout)
	if err != nil {
		log.Printf("error converting Processing API Timeout config value to time: %s", err)
		timeout = 0
	}
	cfg.SourceApi.ClientTimeout = timeout

	timeout, err = time.ParseDuration(f.StorageApi.Timeout)
	if err != nil {
		log.Printf("error converting Storage API Timeout config value to time: %s", err)
		timeout = 0
	}
	cfg.SourceApi.ClientTimeout = timeout

	val, err = strconv.Atoi(f.ProcessingApi.WorkersCount)
	if err != nil {
		log.Print("error converting processing workers count config value. Setting to default")
		val = 0
	}
	cfg.DefaultWorkersCount = val

	val, err = strconv.Atoi(f.StorageApi.WorkersCount)
	if err != nil {
		log.Print("error converting storage workers count config value. Setting to default")
		val = 0
	}
	cfg.DefaultWorkersCount = val
}

func readConfig(cfg *engine.Config) {
	configData, err := os.ReadFile("/etc/config/config.yaml")
	fmt.Println("config data:", string(configData))
	if err != nil {
		log.Fatalf("error reading config from '/etc/config/config.yaml': %s", err)
	}
	err = yaml.Unmarshal(configData, &cfg)
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
