package engine

import (
	"log"
	"time"
)

type Config struct {
	DefaultClientTimeout time.Duration `yaml:"defaultClientTimeout"`
	DefaultWorkersCount  int           `yaml:"defaultWorkersCount"`
	SourceApi            struct {
		URL               string        `yaml:"baseUrl"`
		AuthToken         string        `yaml:"authToken"`
		ClientTimeout     time.Duration `yaml:"timeout"`
		RateLimit         int           `yaml:"rateLimit"`
		RateLimitDuration int           `yaml:"rateLimitPeriodSecs"`
	} `yaml:"sourceApi"`
	ProcessingApi struct {
		URL          string        `yaml:"baseUrl"`
		Timeout      time.Duration `yaml:"timeout"`
		WorkersCount int           `yaml:"workersCount"`
	} `yaml:"processingApi"`
	StorageApi struct {
		URL          string        `yaml:"baseUrl"`
		Timeout      time.Duration `yaml:"timeout"`
		WorkersCount int           `yaml:"workersCount"`
	} `yaml:"storageApi"`
}

type CollectionEngine struct {
	Cfg               Config
	ProcessingService *ProcessingService
	SourceService     *SourceService
	RetryService      *RetryService
}

type Message struct {
	ID           string   `json:"id"`
	Source       string   `json:"source"`
	Title        string   `json:"title"`
	CreationDate string   `json:"creation_date"`
	Message      string   `json:"string"`
	Tags         []string `json:"tags"`
	Author       string   `json:"author"`
}

func (m *Message) GetID() string {
	return m.ID
}

type Payload interface {
	GetID() string
}

func buildSourceConfig(cfg *Config) *SourceServiceConfig {
	return &SourceServiceConfig{
		AuthToken:         cfg.SourceApi.AuthToken,
		ClientTimeout:     cfg.SourceApi.ClientTimeout,
		RateLimitDuration: (time.Duration(cfg.SourceApi.RateLimitDuration) * time.Second),
		RetryWaitTime:     (500 * time.Millisecond),
		RequestsLimit:     cfg.SourceApi.RateLimit,
		URL:               cfg.SourceApi.URL,
	}
}

func buildProcessingConfig(cfg *Config) *ProcessingServiceConfig {
	return &ProcessingServiceConfig{
		ClientTimeout: cfg.ProcessingApi.Timeout,
		URL:           cfg.ProcessingApi.URL,
		WorkerCount:   cfg.ProcessingApi.WorkersCount,
	}
}

func buildRetryConfig(pClient *ProcessingClient, retries chan *Retry) *RetryConfig {
	return &RetryConfig{
		ProcessingClient: pClient,
		Retries:          retries,
	}
}

func NewCollectionEngine(cfg *Config) *CollectionEngine {
	sourceCfg := buildSourceConfig(cfg)
	processingCfg := buildProcessingConfig(cfg)

	source, err := NewSourceService(sourceCfg)
	if err != nil {
		log.Fatal(err)
	}
	// create retry queue to be passed to processing and storge services
	retries := make(chan *Retry)

	// attached upstream and downstream queues to processing service
	processingCfg.Messages = source.Messages
	processingCfg.Retries = retries
	processing, err := NewProcessingService(processingCfg)
	if err != nil {
		log.Fatal(err)
	}
	retryCfg := buildRetryConfig(processing.Client, retries)
	retryService, err := NewRetryService(retryCfg)
	if err != nil {
		log.Fatal(err)
	}

	return &CollectionEngine{
		SourceService:     source,
		ProcessingService: processing,
		RetryService:      retryService,
	}
}

func (ce *CollectionEngine) Run(cancel chan bool) {
	go ce.ProcessingService.Run()
	go ce.SourceService.Run(cancel)
	go ce.RetryService.Run(cancel)
}
