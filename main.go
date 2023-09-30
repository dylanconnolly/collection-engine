package main

import (
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
	DefaultClientTimeout    string `yaml:"defaultClientTimeout"`
	DefaultWorkersCount     string `yaml:"defaultWorkersCount"`
	SourceURL               string `yaml:"sourceApiBaseUrl"`
	SourceAuthToken         string `yaml:"sourceApiAuthToken"`
	SourceTimeout           string `yaml:"sourceClientTimeout"`
	SourceRateLimit         string `yaml:"sourceApiRateLimit"`
	SourceRateLimitDuration string `yaml:"sourceApiRateLimitPeriodSecs"`
	ProcessingURL           string `yaml:"processingApiBaseUrl"`
	ProcessingTimeout       string `yaml:"processingClientTimeout"`
	ProcessingWorkersCount  string `yaml:"processingWorkersCount"`
	StorageURL              string `yaml:"storageApiBaseUrl"`
	StorageTimeout          string `yaml:"storageClientTimeout"`
	StorageWorkersCount     string `yaml:"storageWorkersCount"`
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
	log.Printf("config being set to: %+v", cfg)

	cfg.SourceApi.URL = f.SourceURL
	cfg.SourceApi.AuthToken = f.SourceAuthToken
	cfg.ProcessingApi.URL = f.ProcessingURL
	cfg.StorageApi.URL = f.StorageURL

	timeout, err := time.ParseDuration(f.SourceTimeout)
	if err != nil {
		log.Printf("error converting Source Client Timeout config value to time: %s", err)
		timeout = 0
	}
	cfg.SourceApi.ClientTimeout = timeout

	val, err = strconv.Atoi(f.SourceRateLimit)
	if err != nil {
		log.Printf("error converting Source API Rate Limit config value to int: %s", err)
		val = 0
	}
	cfg.SourceApi.RateLimit = val

	val, err = strconv.Atoi(f.SourceRateLimitDuration)
	if err != nil {
		log.Printf("error converting Source API Rate Limit Duration config value to int: %s", err)
		val = 0
	}
	cfg.SourceApi.RateLimitDuration = val

	timeout, err = time.ParseDuration(f.ProcessingTimeout)
	if err != nil {
		log.Printf("error converting Processing API Timeout config value to time: %s", err)
		timeout = 0
	}
	cfg.SourceApi.ClientTimeout = timeout

	timeout, err = time.ParseDuration(f.StorageTimeout)
	if err != nil {
		log.Printf("error converting Storage API Timeout config value to time: %s", err)
		timeout = 0
	}
	cfg.SourceApi.ClientTimeout = timeout

	val, err = strconv.Atoi(f.ProcessingWorkersCount)
	if err != nil {
		log.Print("error converting processing workers count config value. Setting to default")
		val = 0
	}
	cfg.ProcessingApi.WorkersCount = val

	val, err = strconv.Atoi(f.StorageWorkersCount)
	if err != nil {
		log.Print("error converting storage workers count config value. Setting to default")
		val = 0
	}
	cfg.StorageApi.WorkersCount = val
}

func readConfig(cfg *engine.Config) {
	configData, err := os.ReadFile("/etc/config/config.yaml")
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
		log.Fatal("FATAL: Must set Source API base url in helm/config.yaml. Stopping execution.")
	}
	if cfg.SourceApi.AuthToken == "" {
		log.Fatal("FATAL: Must set Source API auth token in helm/config.yaml. Stopping execution.")
	}
	if cfg.SourceApi.ClientTimeout == 0 {
		cfg.SourceApi.ClientTimeout = cfg.DefaultClientTimeout
	}
	if cfg.SourceApi.RateLimitDuration == 0 {
		cfg.SourceApi.RateLimitDuration = 1
	}
	if cfg.ProcessingApi.URL == "" {
		log.Fatal("FATAL: Must set Processing API base url in helm/config.yaml. Stopping execution.")
	}
	if cfg.ProcessingApi.Timeout == 0 {
		cfg.ProcessingApi.Timeout = cfg.DefaultClientTimeout
	}
	if cfg.ProcessingApi.WorkersCount == 0 {
		cfg.ProcessingApi.WorkersCount = cfg.DefaultWorkersCount
	}
	if cfg.StorageApi.URL == "" {
		log.Fatal("FATAL: Must set Storage API base url in helm/config.yaml. Stopping execution.")
	}
	if cfg.StorageApi.Timeout == 0 {
		cfg.StorageApi.Timeout = cfg.DefaultClientTimeout
	}
	if cfg.StorageApi.WorkersCount == 0 {
		cfg.StorageApi.WorkersCount = cfg.DefaultWorkersCount
	}
}
