package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const (
	backoffDuration = (30 * time.Second)
	errWaitTime     = (500 * time.Millisecond)
)

type ApiClient struct {
	AuthToken     string
	Cursor        *int
	HttpClient    *http.Client
	RetryWaitTime time.Duration
	RequestsLimit int
	RequestsCount int
	URL           string
}

type SourceService struct {
	Client   *ApiClient
	Cursor   *int
	Messages chan []Message
	Ticker   time.Ticker
}

func (s *SourceService) SetUrl(url string) {
	s.Client.URL = url
}

type MessageResponse struct {
	Results []Message `json:"results"`
	Cursor  *int      `json:"cursor"`
}

type HttpError struct {
	StatusCode int
	Message    string
}

type SourceServiceConfig struct {
	AuthToken         string
	ClientTimeout     time.Duration
	RateLimitDuration time.Duration
	RetryWaitTime     time.Duration
	RequestsLimit     int
	URL               string
}

func NewSourceService(cfg *SourceServiceConfig) (*SourceService, error) {
	if cfg.AuthToken == "" || cfg.URL == "" {
		return nil, fmt.Errorf("Source service config: AuthToken and URL cannot be blank. AuthToken: '%v', URL: '%v'", cfg.AuthToken, cfg.URL)
	}

	if cfg.ClientTimeout == 0 {
		return nil, fmt.Errorf("Source service config: ClientTimeout cannot be 0. ClientTimeout: %v", cfg.ClientTimeout)
	}
	return &SourceService{
		Client: &ApiClient{
			URL:           cfg.URL,
			AuthToken:     cfg.AuthToken,
			HttpClient:    &http.Client{Timeout: cfg.ClientTimeout},
			RequestsLimit: cfg.RequestsLimit,
		},
		Messages: make(chan []Message),
		Ticker:   *time.NewTicker(cfg.RateLimitDuration),
	}, nil
}

func (c *ApiClient) getMessages() ([]Message, error) {
	var msgResp MessageResponse

	if c.RequestsLimit > 0 && c.RequestsCount >= c.RequestsLimit {
		return nil, fmt.Errorf("Reached requests per minute limit, waiting to reissue requests")
	}

	url := c.URL + "/messages"
	if c.Cursor != nil {
		url = url + "/" + fmt.Sprint(*c.Cursor)
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	fmt.Println("requesting")
	if err != nil {
		return nil, fmt.Errorf("error creating request %s", err)
	}

	req.Header.Add("X-Auth-Token", c.AuthToken)
	resp, err := c.HttpClient.Do(req)
	c.RequestsCount++
	if err != nil {
		return nil, fmt.Errorf("error sending client request: %s", err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("source api returned status: %v, requestUrl: %v, headers: %v, responseBody: %v", resp.Status, resp.Request.URL, resp.Request.Header, string(body))
		return nil, NewHttpError(resp.StatusCode, errMsg)
	}

	err = json.Unmarshal(body, &msgResp)
	if err != nil {
		return nil, fmt.Errorf("Could not unmarshal GET /messages response into Message struct, response body: %v", string(body))
	}

	c.Cursor = msgResp.Cursor

	if len(msgResp.Results) < 1 {
		return nil, fmt.Errorf("WARN: Empty results array received from source API. Waiting %v", errWaitTime)
	}

	return msgResp.Results, nil
}

func (ss *SourceService) HandleGetMessages() []Message {
	msgs, err := ss.Client.getMessages()
	if err != nil {
		ss.HandleError(err)
		return nil
	}

	return msgs
}

func (ss *SourceService) Run(cancel <-chan bool) {
	log.Println("Source service started.")
	for {
		select {
		case <-ss.Ticker.C:
			ss.Client.RequestsCount = 0
		case <-cancel:
			log.Println("Source Service received cancel signal. Stopping service.")
			close(ss.Messages)
			return
		case ss.Messages <- ss.HandleGetMessages():
		}
	}
}

func (e *HttpError) Error() string {
	return fmt.Sprintf("err: %v}", e.Message)
}

func NewHttpError(statusCode int, message string) *HttpError {
	return &HttpError{
		StatusCode: statusCode,
		Message:    message,
	}
}

func (ss *SourceService) HandleError(err error) {
	if errResp, ok := err.(*HttpError); ok {
		switch errResp.StatusCode {
		case 500:
			log.Println(errResp)
			// time.Sleep(errWaitTime)
		case 429:
			log.Println(errResp)
			time.Sleep(backoffDuration)
		case 401:
			log.Println(errResp)
			// time.Sleep(errWaitTime)
		default:
			log.Println(errResp)
			// time.Sleep(errWaitTime)
		}
		time.Sleep(errWaitTime)
	} else {
		log.Print(err)
		time.Sleep(errWaitTime)
	}
}
