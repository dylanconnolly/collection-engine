package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type ProcessingClient struct {
	URL        string
	HttpClient *http.Client
}

type ProcessingService struct {
	Client *ProcessingClient
	WorkerPool
	ProcessedMessages chan *ProcessedMessage
	Retries           chan *Retry
}

func (ps *ProcessingService) SetUrl(url string) {
	ps.Client.URL = url
}

type WorkerPool struct {
	count int
	Jobs  chan []Message
}

type ProcessingServiceConfig struct {
	URL           string
	ClientTimeout time.Duration
	WorkerCount   int
	Messages      chan []Message
	Retries       chan *Retry
}

func NewProcessingService(cfg *ProcessingServiceConfig) (*ProcessingService, error) {
	if cfg.URL == "" || cfg.ClientTimeout == 0 || cfg.WorkerCount == 0 {
		return nil, fmt.Errorf("Processing service config: ClientTimeout or WorkerCount cannot be 0, URL cannot be empty. ClientTimeout: %v, WorkerCount: %v, URL: '%v'", cfg.ClientTimeout, cfg.WorkerCount, cfg.URL)
	}

	if cfg.Messages == nil || cfg.Retries == nil {
		return nil, fmt.Errorf("Processing service config: upstream and downstream channels cannot be nil. Messages: %v, Retries: %v", cfg.Messages, cfg.Retries)
	}

	return &ProcessingService{
		Client: &ProcessingClient{
			URL:        cfg.URL,
			HttpClient: &http.Client{Timeout: cfg.ClientTimeout},
		},
		WorkerPool:        NewPool(cfg.WorkerCount, cfg.Messages),
		ProcessedMessages: make(chan *ProcessedMessage),
		Retries:           cfg.Retries,
	}, nil
}

func NewPool(count int, jobsChannel chan []Message) WorkerPool {
	return WorkerPool{
		count: count,
		Jobs:  jobsChannel,
	}
}

func (c *ProcessingClient) PostMessage(msg Payload) (*ProcessedMessage, error) {
	var processedMsg ProcessedMessage
	payload, err := json.Marshal(msg)

	if err != nil {
		log.Printf("error marshalling Message before sending to processing. Error: %s", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.URL+"/message", bytes.NewBuffer(payload))

	if err != nil {
		log.Printf("error creating POST message request to processing service. Error: %s", err)
		return nil, err
	}
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		log.Printf("error posting message to processing client. Error: %s", err)
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		err = errors.New(fmt.Sprintf("received non 200 response from processing api: '%s, body: %s'", resp.Status, string(body)))
		return nil, err
	}

	err = json.Unmarshal(body, &processedMsg)
	if err != nil {
		return nil, err
	}

	return &processedMsg, nil
}

func (ps *ProcessingService) ProcessMessage(msg *Message) {
	processedMsg, err := ps.Client.PostMessage(msg)
	if err != nil {
		log.Printf("error for messageID='%s', sending to retry queue. err: %s", msg.ID, err)
		var r Retry
		r.New("processing", msg, ps.ProcessedMessages)
		ps.Retries <- &r
		return
	}
	ps.ProcessedMessages <- processedMsg
}

func (ps *ProcessingService) Run() {
	log.Printf("Processing Service started with %d workers", ps.WorkerPool.count)
	var wg sync.WaitGroup
	for i := 0; i < ps.WorkerPool.count; i++ {
		wg.Add(1)
		i := i
		go ps.processJob(i, &wg, ps.WorkerPool.Jobs)
	}
	wg.Wait()
	close(ps.ProcessedMessages)
}

func (ps *ProcessingService) processJob(id int, wg *sync.WaitGroup, jobs <-chan []Message) {
	defer wg.Done()
	for j := range jobs {
		for _, msg := range j {
			msg := msg
			ps.ProcessMessage(&msg)
		}
	}
}
