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

type StorageClient struct {
	URL        string
	HttpClient *http.Client
}

type StorageService struct {
	Client *StorageClient
	StorageWorkerPool
	Retries chan *Retry
}

func (ss *StorageService) SetUrl(url string) {
	ss.Client.URL = url
}

type StorageWorkerPool struct {
	count int
	Jobs  chan *ProcessedMessage
}

type StorageServiceConfig struct {
	URL               string
	ClientTimeout     time.Duration
	WorkerCount       int
	ProcessedMessages chan *ProcessedMessage
	Retries           chan *Retry
}

func NewStorageService(cfg *StorageServiceConfig) (*StorageService, error) {
	if cfg.URL == "" || cfg.ClientTimeout == 0 || cfg.WorkerCount == 0 {
		return nil, fmt.Errorf("Storage service config: ClientTimeout or WorkerCount cannot be 0, URL cannot be empty. ClientTimeout: %v, WorkerCount: %v, URL: '%v'", cfg.ClientTimeout, cfg.WorkerCount, cfg.URL)
	}

	if cfg.ProcessedMessages == nil || cfg.Retries == nil {
		return nil, fmt.Errorf("Storage service config: upstream and downstream channels cannot be nil. ProcessedMessages: %v, Retries: %v", cfg.ProcessedMessages, cfg.Retries)
	}

	return &StorageService{
		Client: &StorageClient{
			URL:        cfg.URL,
			HttpClient: &http.Client{Timeout: cfg.ClientTimeout},
		},
		StorageWorkerPool: NewStoragePool(cfg.WorkerCount, cfg.ProcessedMessages),
		Retries:           cfg.Retries,
	}, nil
}

func NewStoragePool(count int, jobsChannel chan *ProcessedMessage) StorageWorkerPool {
	return StorageWorkerPool{
		count: count,
		Jobs:  jobsChannel,
	}
}

func (c *StorageClient) PostMessage(processedMsg Payload) error {
	payload, err := json.Marshal(processedMsg)

	if err != nil {
		log.Printf("error marshalling ProcessedMessage before sending to storage. Error: %s", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.URL+"/message", bytes.NewBuffer(payload))

	if err != nil {
		log.Printf("error creating POST message request to storage service. Error: %s", err)
		return err
	}
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		log.Printf("error posting message to storage service. Error: %s", err)
		return err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusCreated {
		err = errors.New(fmt.Sprintf("received non 201 response from storage api: '%s, body: %s'", resp.Status, string(body)))
		return err
	}

	return nil
}

func (ss *StorageService) StoreMessage(processedMsg *ProcessedMessage) {
	err := ss.Client.PostMessage(processedMsg)
	if err != nil {
		log.Printf("error for messageID='%s', sending to retry queue. err: %s", processedMsg.ID, err)
		var r Retry
		r.New("storage", processedMsg, nil)
		ss.Retries <- &r
		return
	}
	log.Printf("storage successful for messageID='%s'", processedMsg.ID)
	return
}

func (ss *StorageService) Run() {
	var wg sync.WaitGroup
	for i := 0; i < ss.StorageWorkerPool.count; i++ {
		wg.Add(1)
		i := i
		go ss.processJob(i, &wg, ss.StorageWorkerPool.Jobs)
	}
	wg.Wait()
}

func (ss *StorageService) processJob(id int, wg *sync.WaitGroup, jobs <-chan *ProcessedMessage) {
	defer wg.Done()
	for j := range jobs {
		msg := j
		ss.StoreMessage(msg)
	}
}
