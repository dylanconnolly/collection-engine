package engine_test

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dylanconnolly/collection-engine/engine"
	"github.com/dylanconnolly/collection-engine/test_utils"
)

var scfg = engine.StorageServiceConfig{
	URL:               "testurl",
	ClientTimeout:     (5 * time.Second),
	WorkerCount:       3,
	ProcessedMessages: make(chan *engine.ProcessedMessage),
	Retries:           make(chan *engine.Retry),
}

func TestNewStorageService(t *testing.T) {
	t.Run("should return a pointer to new service", func(t *testing.T) {
		_, err := engine.NewStorageService(&scfg)
		if err != nil {
			t.Error("new config should not return error when all fields are set.")
		}
	})

	t.Run("bad configs", func(t *testing.T) {
		t.Run("missing URL", func(t *testing.T) {
			badConfig := engine.StorageServiceConfig{
				ClientTimeout:     (5 * time.Second),
				WorkerCount:       3,
				ProcessedMessages: make(chan *engine.ProcessedMessage),
			}
			_, err := engine.NewStorageService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Storage service config: ClientTimeout or WorkerCount cannot be 0, URL cannot be empty. ClientTimeout: %v, WorkerCount: %v, URL: '%v'", badConfig.ClientTimeout, badConfig.WorkerCount, badConfig.URL)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
		t.Run("missing ClientTimeout", func(t *testing.T) {
			badConfig := engine.StorageServiceConfig{
				URL:               "testurl",
				WorkerCount:       3,
				ProcessedMessages: make(chan *engine.ProcessedMessage),
			}
			_, err := engine.NewStorageService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Storage service config: ClientTimeout or WorkerCount cannot be 0, URL cannot be empty. ClientTimeout: %v, WorkerCount: %v, URL: '%v'", badConfig.ClientTimeout, badConfig.WorkerCount, badConfig.URL)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
		t.Run("missing WorkerCount", func(t *testing.T) {
			badConfig := engine.StorageServiceConfig{
				URL:               "testurl",
				ClientTimeout:     (5 * time.Second),
				ProcessedMessages: make(chan *engine.ProcessedMessage),
			}
			_, err := engine.NewStorageService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Storage service config: ClientTimeout or WorkerCount cannot be 0, URL cannot be empty. ClientTimeout: %v, WorkerCount: %v, URL: '%v'", badConfig.ClientTimeout, badConfig.WorkerCount, badConfig.URL)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
		t.Run("missing Messages channel", func(t *testing.T) {
			badConfig := engine.StorageServiceConfig{
				URL:           "testurl",
				ClientTimeout: (5 * time.Second),
				WorkerCount:   3,
			}
			_, err := engine.NewStorageService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Storage service config: upstream and downstream channels cannot be nil. ProcessedMessages: %v, Retries: %v", badConfig.ProcessedMessages, badConfig.Retries)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
		t.Run("missing Retries channel", func(t *testing.T) {
			badConfig := engine.StorageServiceConfig{
				URL:               "testurl",
				ClientTimeout:     (5 * time.Second),
				WorkerCount:       3,
				ProcessedMessages: make(chan *engine.ProcessedMessage),
			}
			_, err := engine.NewStorageService(&badConfig)
			if err == nil {
				t.Error("new config should not return error when all fields are set.")
			}
			expected := fmt.Sprintf("Storage service config: upstream and downstream channels cannot be nil. ProcessedMessages: %v, Retries: %v", badConfig.ProcessedMessages, badConfig.Retries)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
	})
}

func TestStorageClient(t *testing.T) {
	var processed = engine.ProcessedMessage{
		test_utils.GenerateMockMessages(1)[0],
		time.Now().UTC().String(),
	}
	t.Run("PostMessage should return errors received from server", func(t *testing.T) {
		ss, _ := engine.NewStorageService(&scfg)

		ts := test_utils.CreateTestServer(ss, "/messages", "error message", 500)
		defer ts.Close()

		err := ss.Client.PostMessage(&processed)
		if err == nil {
			t.Error("expected an error to be returned")
		}
	})

	t.Run("PostMessage should return nil if storage response is successful", func(t *testing.T) {
		ss, _ := engine.NewStorageService(&scfg)

		ts := test_utils.CreateTestServer(ss, "/messages", "created", 201)
		defer ts.Close()

		err := ss.Client.PostMessage(&processed)
		if err != nil {
			t.Error("error should be nil on successful post")
		}
	})
}

func TestStoreMessage(t *testing.T) {
	t.Run("when a no error is returned, message should be logged and job should not be added to retries", func(t *testing.T) {
		var processed = engine.ProcessedMessage{
			test_utils.GenerateMockMessages(1)[0],
			time.Now().UTC().String(),
		}

		ss, _ := engine.NewStorageService(&scfg)

		ts := test_utils.CreateTestServer(ss, "/messages", processed, 201)
		defer ts.Close()
		// collect log output
		var buf bytes.Buffer
		log.SetOutput(&buf)
		defer func() {
			log.SetOutput(os.Stderr)
		}()

		ss.StoreMessage(&processed)
		l := len(ss.Retries)
		if l > 0 {
			t.Errorf("expected retry queue length to be 0, got %d", l)
		}
		t.Log(buf.String())
		expected := fmt.Sprintf("storage successful for messageID='%s'", processed.ID)
		if !strings.Contains(buf.String(), expected) {
			t.Errorf("expected successful storage to produce log output: '%s', got: '%s'", expected, buf.String())
		}
	})

	t.Run("if an error is returned by client ProcessedMessage should be added to retry", func(t *testing.T) {
		processed := engine.ProcessedMessage{
			test_utils.GenerateMockMessages(1)[0],
			time.Now().UTC().String(),
		}

		ss, _ := engine.NewStorageService(&scfg)

		ts := test_utils.CreateTestServer(ss, "/messages", "error encountered", 401)
		defer ts.Close()

		go ss.StoreMessage(&processed)
		retry := <-ss.Retries
		if retry == nil {
			t.Error("processing errors should add the message to retries channel")
		}
		id := retry.Payload.GetID()
		if id != processed.ID {
			t.Errorf("failed processing message added to retries should match message sent to processing service. Expected: '%s', got: '%s'", processed.ID, id)
		}
	})
}

func TestRunStorageService(t *testing.T) {
	pmsg := engine.ProcessedMessage{
		test_utils.GenerateMockMessages(1)[0],
		time.Now().UTC().String(),
	}
	ss, _ := engine.NewStorageService(&scfg)
	ss.Retries = make(chan *engine.Retry, 50)
	ts := test_utils.CreateTestServer(ss, "/messages", "created", 201)
	defer ts.Close()
	go ss.Run()

	// switch log output so it doesnt spam terminal
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	for i := 0; i < 50; i++ {
		ss.StorageWorkerPool.Jobs <- &pmsg
	}
	if len(ss.StorageWorkerPool.Jobs) == 0 {
		close(ss.StorageWorkerPool.Jobs)
	}

	if len(ss.Retries) > 0 {
		t.Error("no successful jobs should be sent to retries queue")
	}
}
