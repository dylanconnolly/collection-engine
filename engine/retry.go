package engine

import (
	"fmt"
	"log"
)

type RetryService struct {
	ProcessingClient *ProcessingClient
	Retries          chan *Retry
	StorageClient    *StorageClient
}

type Retry struct {
	RetryCount    int
	MaxRetries    int
	ServiceName   string
	Payload       Payload
	OutputChannel chan *ProcessedMessage
}

func (r *Retry) New(service string, payload Payload, channel chan *ProcessedMessage) {
	r.MaxRetries = 2
	r.ServiceName = service
	r.Payload = payload
	r.OutputChannel = channel
}

type RetryConfig struct {
	ProcessingClient *ProcessingClient
	StorageClient    *StorageClient
	Retries          chan *Retry
}

func NewRetryService(cfg *RetryConfig) (*RetryService, error) {
	if cfg.ProcessingClient == nil || cfg.Retries == nil || cfg.StorageClient == nil {
		return nil, fmt.Errorf("Retry service config: clients or Retries cannot be nil. ProcessingClient: %v, StorageClient: %v, Retries: %v", cfg.ProcessingClient, cfg.StorageClient, cfg.Retries)
	}
	return &RetryService{
		ProcessingClient: cfg.ProcessingClient,
		StorageClient:    cfg.StorageClient,
		Retries:          cfg.Retries,
	}, nil
}

func (rs *RetryService) Run(cancel chan bool) {
	for {
		select {
		case r := <-rs.Retries:
			rs.ProcessRetry(r)
		case <-cancel:
			log.Println("cancel directive received. Retry service shutting down.")
			return
		}
	}
}

func (rs *RetryService) ProcessRetry(r *Retry) {
	if r.RetryCount >= r.MaxRetries {
		log.Printf("max retry count reached for messageID='%s'", r.Payload.GetID())
		log.Printf("FAILED: %s for messageID='%s' failed.", r.ServiceName, r.Payload.GetID())
		return
	}

	if r.ServiceName == "processing" {
		pmsg, err := rs.ProcessingClient.PostMessage(r.Payload)
		r.RetryCount++
		if err != nil {
			log.Printf("retry attempt %d for messageID='%s' failed", r.RetryCount, r.Payload.GetID())
			rs.ProcessRetry(r)
		}
		if pmsg != nil {
			r.OutputChannel <- pmsg
			return
		}
	}

	if r.ServiceName == "storage" {
		err := rs.StorageClient.PostMessage(r.Payload)
		r.RetryCount++
		if err != nil {
			log.Printf("retry attempt %d for messageID='%s' failed", r.RetryCount, r.Payload.GetID())
			rs.ProcessRetry(r)
		}
		return
	}
}
