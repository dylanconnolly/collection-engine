package engine_test

import (
	"collection-engine/engine"
	"collection-engine/test_utils"
	"fmt"
	"testing"
	"time"
)

var tProcessingCfg = engine.ProcessingServiceConfig{
	URL:           "testurl",
	ClientTimeout: (5 * time.Second),
	WorkerCount:   3,
	Messages:      make(chan []engine.Message),
	Retries:       make(chan *engine.Retry),
}

var testProcessingService, cfgerr = engine.NewProcessingService(&tProcessingCfg)

var retryCfg = engine.RetryConfig{
	ProcessingClient: testProcessingService.Client,
	Retries:          make(chan *engine.Retry),
}

func TestNewRetryService(t *testing.T) {
	t.Run("should return a new service", func(t *testing.T) {
		s, err := engine.NewRetryService(&retryCfg)
		if err != nil {
			t.Error("new config should not return error when all fields are set.")
		}

		if s == nil {
			t.Error("service should not be nil")
		}
	})

	t.Run("should return error if config is missing values", func(t *testing.T) {
		badConfig := engine.RetryConfig{}

		_, err := engine.NewRetryService(&badConfig)
		if err == nil {
			t.Error("should return error when config is missing values")
		}
	})
}

func TestProcessRetry(t *testing.T) {
	msg := test_utils.GenerateMockMessages(1)[0]
	pmsg := engine.ProcessedMessage{
		msg,
		time.Now().UTC().String(),
	}
	var retry = engine.Retry{
		MaxRetries:  2,
		ServiceName: "processing",
		Payload:     &msg,
		// OutputChannel: testProcessingService.ProcessedMessages,
		OutputChannel: make(chan *engine.ProcessedMessage, 5),
	}
	t.Run("should retry on failures until max retry amount is met", func(t *testing.T) {
		r, _ := engine.NewRetryService(&retryCfg)

		ts := test_utils.CreateTestServer(testProcessingService, "/messages", "error response", 500)
		defer ts.Close()
		retryCopy := retry
		r.ProcessRetry(&retryCopy)

		if len(retry.OutputChannel) > 0 {
			t.Error("should not add to output queue on failures")
		}
	})

	t.Run("should add successful response to output queue and stop running retry job", func(t *testing.T) {
		r, _ := engine.NewRetryService(&retryCfg)

		ts := test_utils.CreateTestServer(testProcessingService, "/messages", pmsg, 200)
		defer ts.Close()

		retryCopy := retry
		r.ProcessRetry(&retryCopy)

		if len(retry.OutputChannel) != 1 {
			t.Error("only one output should be added to queue")
		}
		output := <-retry.OutputChannel
		fmt.Printf("%+v", output)

		if output.ID != pmsg.ID {
			t.Error("output from retry should match the processed massage server returned")
		}
		if output.ProcessingDate != pmsg.ProcessingDate {
			t.Error("output should be a processed message with a ProcessingDate value")
		}
	})
}
