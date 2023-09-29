package test_utils

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/dylanconnolly/collection-engine/engine"
)

type Service interface {
	SetUrl(u string)
}

func GenerateMockMessages(count int) []engine.Message {
	var messages []engine.Message

	for i := 1; i <= count; i++ {
		i := i
		msg := engine.Message{
			ID:     fmt.Sprintf("message-id-%d", i),
			Source: "MessagingSystem",
			Title:  fmt.Sprintf("Message Title %d", i),
			// CreationDate: time.Now(),
			CreationDate: "2030-08-24T17:16:52.228009",
			Message:      fmt.Sprintf("test message %d", i),
			Tags: []string{
				"random",
				"tags",
				"generated",
				"by test",
			},
			Author: fmt.Sprintf("Test Author %d", i),
		}

		messages = append(messages, msg)
	}

	return messages
}

func GenerateBatchMessages(batchSize, batchCount int) [][]engine.Message {
	var batches [][]engine.Message
	msgs := GenerateMockMessages(batchSize * batchCount)
	for i := 0; i < len(msgs); i += batchSize {
		end := i + batchSize

		if end > len(msgs) {
			end = len(msgs)
		}
		batches = append(batches, msgs[i:end])
	}
	return batches
}

func CreateTestServer(svc Service, path string, resp interface{}, statusCode int) *httptest.Server {
	r, err := json.Marshal(resp)
	if err != nil {
		log.Fatalf("error marshalling mock response in server setup: %s", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(statusCode)
		w.Write(r)
	}))

	svc.SetUrl(ts.URL)
	ts.URL = ts.URL + path
	return ts
}

func BuildCollectionEngineConfig(workers, rateLimit, rateLimitDuration int) *engine.Config {
	duration := (5 * time.Second)
	cfg := engine.Config{
		DefaultClientTimeout: duration,
		DefaultWorkersCount:  3,
		SourceApi: struct {
			URL               string        "yaml:\"baseUrl\""
			AuthToken         string        "yaml:\"authToken\""
			ClientTimeout     time.Duration "yaml:\"timeout\""
			RateLimit         int           "yaml:\"rateLimit\""
			RateLimitDuration int           "yaml:\"rateLimitPeriodSecs\""
		}{URL: "test", AuthToken: "test", ClientTimeout: duration, RateLimit: rateLimit, RateLimitDuration: rateLimitDuration},
		ProcessingApi: struct {
			URL          string        "yaml:\"baseUrl\""
			Timeout      time.Duration "yaml:\"timeout\""
			WorkersCount int           "yaml:\"workersCount\""
		}{URL: "test", Timeout: duration, WorkersCount: workers},
		StorageApi: struct {
			URL          string        "yaml:\"baseUrl\""
			Timeout      time.Duration "yaml:\"timeout\""
			WorkersCount int           "yaml:\"workersCount\""
		}{},
	}
	return &cfg
}
