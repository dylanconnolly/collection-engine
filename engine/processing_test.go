package engine_test

import (
	"collection-engine/engine"
	"collection-engine/test_utils"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var pcfg = engine.ProcessingServiceConfig{
	URL:           "testurl",
	ClientTimeout: (5 * time.Second),
	WorkerCount:   3,
	Messages:      make(chan []engine.Message),
	Retries:       make(chan *engine.Retry),
}

func TestNewProcessingService(t *testing.T) {
	t.Run("should return a pointer to new service", func(t *testing.T) {
		_, err := engine.NewProcessingService(&pcfg)
		if err != nil {
			t.Error("new config should not return error when all fields are set.")
		}
	})

	t.Run("bad configs", func(t *testing.T) {
		t.Run("missing URL", func(t *testing.T) {
			badConfig := engine.ProcessingServiceConfig{
				ClientTimeout: (5 * time.Second),
				WorkerCount:   3,
				Messages:      make(chan []engine.Message),
			}
			_, err := engine.NewProcessingService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Processing service config: ClientTimeout or WorkerCount cannot be 0, URL cannot be empty. ClientTimeout: %v, WorkerCount: %v, URL: '%v'", badConfig.ClientTimeout, badConfig.WorkerCount, badConfig.URL)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
		t.Run("missing ClientTimeout", func(t *testing.T) {
			badConfig := engine.ProcessingServiceConfig{
				URL:         "testurl",
				WorkerCount: 3,
				Messages:    make(chan []engine.Message),
			}
			_, err := engine.NewProcessingService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Processing service config: ClientTimeout or WorkerCount cannot be 0, URL cannot be empty. ClientTimeout: %v, WorkerCount: %v, URL: '%v'", badConfig.ClientTimeout, badConfig.WorkerCount, badConfig.URL)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
		t.Run("missing WorkerCount", func(t *testing.T) {
			badConfig := engine.ProcessingServiceConfig{
				URL:           "testurl",
				ClientTimeout: (5 * time.Second),
				Messages:      make(chan []engine.Message),
			}
			_, err := engine.NewProcessingService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Processing service config: ClientTimeout or WorkerCount cannot be 0, URL cannot be empty. ClientTimeout: %v, WorkerCount: %v, URL: '%v'", badConfig.ClientTimeout, badConfig.WorkerCount, badConfig.URL)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
		t.Run("missing Messages channel", func(t *testing.T) {
			badConfig := engine.ProcessingServiceConfig{
				URL:           "testurl",
				ClientTimeout: (5 * time.Second),
				WorkerCount:   3,
			}
			_, err := engine.NewProcessingService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Processing service config: upstream and downstream channels cannot be nil. Messages: %v, Retries: %v", badConfig.Messages, badConfig.Retries)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
		t.Run("missing Retries channel", func(t *testing.T) {
			badConfig := engine.ProcessingServiceConfig{
				URL:           "testurl",
				ClientTimeout: (5 * time.Second),
				WorkerCount:   3,
				Messages:      make(chan []engine.Message),
			}
			_, err := engine.NewProcessingService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Processing service config: upstream and downstream channels cannot be nil. Messages: %v, Retries: %v", badConfig.Messages, badConfig.Retries)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
	})
}

func TestProcessingClient(t *testing.T) {
	var msg = test_utils.GenerateMockMessages(1)[0]
	var processed = engine.ProcessedMessage{
		msg,
		time.Now().UTC().String(),
	}
	t.Run("PostMessage should return errors received from server", func(t *testing.T) {
		ps, _ := engine.NewProcessingService(&pcfg)

		ts := test_utils.CreateTestServer(ps, "/messages", "error message", 500)
		defer ts.Close()

		pmsg, err := ps.Client.PostMessage(&msg)
		if err == nil {
			t.Error("expected an error to be returned")
		}
		if pmsg != nil {
			t.Error("expected response to be nil")
		}
	})

	t.Run("PostMessage should return a pointer to processed message if no errors occur", func(t *testing.T) {
		ps, _ := engine.NewProcessingService(&pcfg)

		ts := test_utils.CreateTestServer(ps, "/messages", processed, 200)
		defer ts.Close()

		pmsg, err := ps.Client.PostMessage(&msg)
		if err != nil {
			t.Error("error should be nil on successful post")
		}

		if !cmp.Equal(pmsg.ID, msg.ID) {
			t.Errorf("expected return to match input: Expected: %+v, got: %+v", msg.ID, pmsg.ID)
		}
		if !cmp.Equal(pmsg.Title, msg.Title) {
			t.Errorf("expected return to match input: Expected: %+v, got: %+v", msg.Title, pmsg.Title)
		}
		if !cmp.Equal(pmsg.ProcessingDate, processed.ProcessingDate) {
			t.Errorf("expected return to match input: Expected: %+v, got: %+v", processed.ProcessingDate, pmsg.ProcessingDate)
		}
	})
}

func TestProcessMessage(t *testing.T) {
	t.Run("when a processed message is returned, it should be added to the ProcessedMessages queue", func(t *testing.T) {
		var msg = test_utils.GenerateMockMessages(1)[0]
		var processed = engine.ProcessedMessage{
			msg,
			time.Now().UTC().String(),
		}

		ps, _ := engine.NewProcessingService(&pcfg)
		ps.ProcessedMessages = make(chan *engine.ProcessedMessage, 4)

		ts := test_utils.CreateTestServer(ps, "/messages", processed, 200)
		defer ts.Close()

		ps.ProcessMessage(&msg)
		l := len(ps.ProcessedMessages)
		if l != 1 {
			t.Errorf("expected queue length to be 1, got %d", l)
		}

		ps.ProcessMessage(&msg)
		ps.ProcessMessage(&msg)
		ps.ProcessMessage(&msg)
		l = len(ps.ProcessedMessages)
		if l != 4 {
			t.Errorf("expected queue length to be 4, got %d", l)
		}
	})

	t.Run("if an error is returned by client message should be added to retry", func(t *testing.T) {
		var msg = test_utils.GenerateMockMessages(1)[0]

		ps, _ := engine.NewProcessingService(&pcfg)
		ps.ProcessedMessages = make(chan *engine.ProcessedMessage, 2)

		ts := test_utils.CreateTestServer(ps, "/messages", "error encountered", 401)
		defer ts.Close()

		ps.ProcessMessage(&msg)
		l := len(ps.ProcessedMessages)
		if l != 0 {
			t.Errorf("message should NOT be added to ProcessedMessage queue, got queue length of: %d", l)
		}
	})
}

func TestRunProcessingService(t *testing.T) {
	pmsg := engine.ProcessedMessage{
		test_utils.GenerateMockMessages(1)[0],
		time.Now().UTC().String(),
	}
	batchSize := 5
	batchCount := 10
	ps, _ := engine.NewProcessingService(&pcfg)
	ts := test_utils.CreateTestServer(ps, "/messages", pmsg, 200)
	defer ts.Close()
	go ps.Run()
	batches := test_utils.GenerateBatchMessages(5, 10)
	go func() {
		for _, batch := range batches {
			ps.WorkerPool.Jobs <- batch
		}
		close(ps.WorkerPool.Jobs)
	}()

	var results []*engine.ProcessedMessage
	for r := range ps.ProcessedMessages {
		results = append(results, r)
	}

	if len(results) != (batchSize * batchCount) {
		t.Errorf("processing results return %d, expected %d records returned", len(results), (batchSize * batchCount))
	}
}
