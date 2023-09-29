package engine_test

import (
	"collection-engine/engine"
	"collection-engine/test_utils"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var cfg = engine.SourceServiceConfig{
	AuthToken:         "testtoken",
	ClientTimeout:     (5 * time.Second),
	RateLimitDuration: (60 * time.Second),
	RetryWaitTime:     (500 * time.Millisecond),
	RequestsLimit:     120,
	URL:               "testurl",
}

func TestHandleGetMessages(t *testing.T) {
	t.Run("if no error", func(t *testing.T) {
		mockResp := engine.MessageResponse{
			Results: test_utils.GenerateMockMessages(10),
			Cursor:  nil,
		}
		source, ts := setupServiceAndTestServer(mockResp, 200)
		defer ts.Close()

		res := source.HandleGetMessages()

		if len(res) != len(mockResp.Results) {
			t.Fatalf("expected to return %d messages, got: %d", len(mockResp.Results), len(res))
		}

		for i, msg := range res {
			exp := mockResp.Results[i]
			if !cmp.Equal(msg, exp) {
				t.Fatalf("expected:\n%+v\n to equal:\n %+v", msg, exp)
			}
		}
	})

	t.Run("if error returned from getMessages", func(t *testing.T) {
		mockResp := engine.MessageResponse{
			Results: []engine.Message{},
			Cursor:  nil,
		}
		source, ts := setupServiceAndTestServer(mockResp, 400)
		defer ts.Close()

		res := source.HandleGetMessages()

		if res != nil {
			t.Error("should return nil if error is returned from getMessage")
		}
	})

	t.Run("if API response body is not parseable", func(t *testing.T) {
		mockResp := struct {
			Results struct {
				ID   string
				Name string
			}
			Cursor *int
		}{
			Results: struct {
				ID   string
				Name string
			}{
				ID:   "askjd",
				Name: "test",
			},
			Cursor: nil,
		}
		source, ts := setupServiceAndTestServer(mockResp, 200)
		defer ts.Close()

		res := source.HandleGetMessages()

		if res != nil {
			t.Error("should return nil if error is returned from getMessage")
		}
	})

	t.Run("cursor values", func(t *testing.T) {
		intCursor := 30
		mockResp := engine.MessageResponse{
			Results: []engine.Message{},
		}
		mockResp2 := engine.MessageResponse{
			Results: []engine.Message{},
			Cursor:  &intCursor,
		}

		tests := map[string]struct {
			input    engine.MessageResponse
			expected *int
		}{
			"null cursor": {
				input:    mockResp,
				expected: mockResp.Cursor,
			},
			"integer cursor": {
				input:    mockResp2,
				expected: mockResp2.Cursor,
			},
		}

		for name, test := range tests {
			source, ts := setupServiceAndTestServer(test.input, 200)
			defer ts.Close()

			source.HandleGetMessages()
			if source.Cursor == nil || test.expected == nil {
				if !cmp.Equal(source.Client.Cursor, test.expected) {
					t.Errorf("Test - %s: expected source service cursor value to be: %v, but got :%v", name, test.expected, source.Cursor)
				}
			} else {
				if !cmp.Equal(*source.Client.Cursor, *test.expected) {
					t.Errorf("Test - %s: expected source service cursor value to be: %v, but got: %v", name, *test.expected, *source.Cursor)
				}
			}
		}
	})

	t.Run("if requests limit is 0 should ignore requestsCount", func(t *testing.T) {
		mock := engine.MessageResponse{
			Results: test_utils.GenerateMockMessages(3),
			Cursor:  nil,
		}

		source, ts := setupServiceAndTestServer(mock, 200)
		defer ts.Close()
		source.Client.RequestsLimit = 0
		source.Client.RequestsCount = 20

		resp := source.HandleGetMessages()

		if resp == nil {
			t.Error("handle messages should not return nil messages if request limit is 0")
		}
	})

	t.Run("subsequent requests should assign new cursor value", func(t *testing.T) {
		cursorVal := 10
		input1 := engine.MessageResponse{
			Results: []engine.Message{},
			Cursor:  &cursorVal,
		}
		source, ts := setupServiceAndTestServer(input1, 200)
		defer ts.Close()

		ts.URL = ts.URL + "/" + fmt.Sprint(cursorVal)

		source.HandleGetMessages()
		if *source.Client.Cursor != cursorVal {
			t.Errorf("expected source service cursor value to be: %v, got: %v", cursorVal, *source.Client.Cursor)
		}

		input2 := engine.MessageResponse{
			Results: []engine.Message{},
			Cursor:  nil,
		}

		source2, ts2 := setupServiceAndTestServer(input2, 200)
		initialCursor := 100
		source2.Client.Cursor = &initialCursor
		ts2.URL = ts.URL + "/" + fmt.Sprint(initialCursor)
		source2.HandleGetMessages()
		if source2.Client.Cursor != nil {
			t.Errorf("expected cursor to be set to %v, got %v", nil, *source2.Client.Cursor)
		}
	})
}

func TestRunSourceService(t *testing.T) {
	t.Run("should return results until cancel directive is sent", func(t *testing.T) {
		msgCount := 10
		mockResp := engine.MessageResponse{
			Results: test_utils.GenerateMockMessages(msgCount),
			Cursor:  nil,
		}
		cancel := make(chan bool)

		source, ts := setupServiceAndTestServer(mockResp, 200)
		defer ts.Close()
		go source.Run(cancel)

		// count := 0
		var messageBatches [][]engine.Message
		for i := 0; i < 5; i++ {
			r := <-source.Messages
			messageBatches = append(messageBatches, r)
		}
		cancel <- true

		if len(messageBatches) != 5 {
			t.Errorf("Running source server with successful responses should pass to results channel, expected 5 results, got %d", len(messageBatches))
		}
	})

	t.Run("should increment cursor", func(t *testing.T) {
		msgCount := 10
		batches := 5
		msgs := test_utils.GenerateBatchMessages(msgCount, batches)
		mockResp := engine.MessageResponse{
			Results: nil,
			Cursor:  nil,
		}
		cancel := make(chan bool)

		mux := http.NewServeMux()
		server := httptest.NewServer(mux)
		defer server.Close()

		cfgCopy := cfg
		cfgCopy.URL = server.URL

		source, _ := engine.NewSourceService(&cfgCopy)

		mux.HandleFunc("/messages", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
			cursorVal := 10
			mockResp.Results = msgs[0]
			mockResp.Cursor = &cursorVal
			r, _ := json.Marshal(&mockResp)
			w.Write(r)
		})
		mux.HandleFunc("/messages/10", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
			cursorVal := 20
			mockResp.Results = msgs[1]
			mockResp.Cursor = &cursorVal
			r, _ := json.Marshal(&mockResp)
			w.Write(r)
		})
		mux.HandleFunc("/messages/20", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
			cursorVal := 30
			mockResp.Results = msgs[2]
			mockResp.Cursor = &cursorVal
			r, _ := json.Marshal(&mockResp)
			w.Write(r)
		})
		mux.HandleFunc("/messages/30", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
			cursorVal := 40
			mockResp.Results = msgs[3]
			mockResp.Cursor = &cursorVal
			r, _ := json.Marshal(&mockResp)
			w.Write(r)
		})
		mux.HandleFunc("/messages/40", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
			resp := engine.MessageResponse{
				Results: msgs[4],
				Cursor:  nil,
			}
			r, _ := json.Marshal(&resp)
			w.Write(r)
		})

		go source.Run(cancel)

		var messageBatches [][]engine.Message
		for i := 0; i < batches; i++ {
			r := <-source.Messages
			messageBatches = append(messageBatches, r)
		}
		cancel <- true
		first := messageBatches[0][0].ID
		last := msgs[4][msgCount-1].ID
		expected := msgs[0][0].ID
		if first != expected {
			t.Errorf("expected ID of first batch message to be: %s, got %s", expected, first)
		}
		expected = msgs[batches-1][msgCount-1].ID
		if last != expected {
			t.Errorf("expected ID of first batch message to be: %s, got %s", expected, last)
		}

		for i, batch := range messageBatches {
			if len(batch) != msgCount {
				t.Errorf("mismatch on pre-process batch size and returned batch size. Pre-process size: %d, after processing got: %d", msgCount, len(batch))
			}
			if !cmp.Equal(batch, msgs[i]) {
				t.Error("batch and messages don't line up")
			}
		}

	})

	t.Run("should close channel when cancel directive is received", func(t *testing.T) {
		msgCount := 10
		mockResp := engine.MessageResponse{
			Results: test_utils.GenerateMockMessages(msgCount),
			Cursor:  nil,
		}
		cancel := make(chan bool)

		source, ts := setupServiceAndTestServer(mockResp, 200)
		defer ts.Close()
		go source.Run(cancel)

		count := 0
		var messageBatches [][]engine.Message
		for r := range source.Messages {
			count++
			messageBatches = append(messageBatches, r)
			if count == 1 {
				cancel <- true
				break
			}
		}
		_, ok := <-source.Messages
		if ok {
			t.Error("channel should be closed after cancelling run call")
		}
	})
}

func TestHandleError(t *testing.T) {
	t.Run("should handle 500 error", func(t *testing.T) {
		source, _ := engine.NewSourceService(&cfg)
		start := time.Now()
		source.HandleError(&engine.HttpError{
			StatusCode: 500,
			Message:    "500 error message",
		})
		duration := time.Since(start)
		if duration < (500 * time.Millisecond) {
			t.Errorf("Handle error should sleep for 500ms after recieving an error from API, duration: %v", duration)
		}
		if duration > (505 * time.Millisecond) {
			t.Errorf("Requests should sleep for only 500ms after recieving an error from API, duration: %v", duration)
		}
	})

	t.Run("should handle 401 error", func(t *testing.T) {
		source, _ := engine.NewSourceService(&cfg)
		start := time.Now()
		source.HandleError(&engine.HttpError{
			StatusCode: 401,
			Message:    "401 error message",
		})
		duration := time.Since(start)
		if duration < (500 * time.Millisecond) {
			t.Errorf("Handle error should sleep for 500ms after recieving an error from API, duration: %v", duration)
		}
		if duration > (505 * time.Millisecond) {
			t.Errorf("Requests should sleep for only 500ms after recieving an error from API, duration: %v", duration)
		}
	})

	t.Run("should handle all other non 200 errors", func(t *testing.T) {
		source, _ := engine.NewSourceService(&cfg)
		start := time.Now()
		source.HandleError(&engine.HttpError{
			StatusCode: 508,
			Message:    "508 error message",
		})
		duration := time.Since(start)
		if duration < (500 * time.Millisecond) {
			t.Errorf("Handle error should sleep for 500ms after recieving an error from API, duration: %v", duration)
		}
		if duration > (505 * time.Millisecond) {
			t.Errorf("Requests should sleep for only 500ms after recieving an error from API, duration: %v", duration)
		}
	})

	t.Run("should handle all other errors", func(t *testing.T) {
		source, _ := engine.NewSourceService(&cfg)
		start := time.Now()
		source.HandleError(errors.New("test error"))
		duration := time.Since(start)
		if duration < (500 * time.Millisecond) {
			t.Errorf("Handle error should sleep for 500ms after recieving an error from API, duration: %v", duration)
		}
		if duration > (505 * time.Millisecond) {
			t.Errorf("Requests should sleep for only 500ms after recieving an error from API, duration: %v", duration)
		}
	})
}

func setupServiceAndTestServer(resp interface{}, statusCode int) (*engine.SourceService, *httptest.Server) {
	r, err := json.Marshal(resp)
	if err != nil {
		log.Fatalf("error marshalling mock response in server setup: %s", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(statusCode)
		w.Write(r)
	}))

	newConf := cfg
	newConf.URL = ts.URL

	source, _ := engine.NewSourceService(&newConf)
	ts.URL = ts.URL + "/messages"

	return source, ts
}

func TestNewSourceServer(t *testing.T) {
	s, err := engine.NewSourceService(&cfg)
	if err != nil {
		t.Errorf("should not receive error with proper config, err: %s", err)
	}

	if s.Client.URL != cfg.URL {
		t.Errorf("client url should match config url. Client url: %s, config url: %s", s.Client.URL, cfg.URL)
	}
	if s.Client.AuthToken != cfg.AuthToken {
		t.Errorf("client Authtoken should match config AuthToken. Client AuthToken: %s, config AuthToken: %s", s.Client.AuthToken, cfg.AuthToken)
	}

	t.Run("bad config returns errors", func(t *testing.T) {
		t.Run("no url", func(t *testing.T) {
			badConfig := engine.SourceServiceConfig{
				AuthToken:         "testtoken",
				ClientTimeout:     (5 * time.Second),
				RateLimitDuration: (60 * time.Second),
				RetryWaitTime:     (500 * time.Millisecond),
				RequestsLimit:     120,
			}

			_, err := engine.NewSourceService(&badConfig)
			if err == nil {
				t.Error("expected error when not providing URL to source config.")
			}
			expected := fmt.Sprintf("Source service config: AuthToken and URL cannot be blank. AuthToken: '%v', URL: '%v'", badConfig.AuthToken, badConfig.URL)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})

		t.Run("no auth token", func(t *testing.T) {
			badConfig := engine.SourceServiceConfig{
				ClientTimeout:     (5 * time.Second),
				RateLimitDuration: (60 * time.Second),
				RetryWaitTime:     (500 * time.Millisecond),
				RequestsLimit:     120,
				URL:               "testurl",
			}

			_, err := engine.NewSourceService(&badConfig)
			if err == nil {
				t.Error("expected error when not providing URL to source config.")
			}
			expected := fmt.Sprintf("Source service config: AuthToken and URL cannot be blank. AuthToken: '%v', URL: '%v'", badConfig.AuthToken, badConfig.URL)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})

		t.Run("no client timeout", func(t *testing.T) {
			badConfig := engine.SourceServiceConfig{
				AuthToken:         "testtoken",
				RateLimitDuration: (60 * time.Second),
				RetryWaitTime:     (500 * time.Millisecond),
				RequestsLimit:     120,
				URL:               "testurl",
			}

			_, err = engine.NewSourceService(&badConfig)
			if err == nil {
				t.Error("expected error when not providing ClientTimeout to source config.")
			}
			expected := fmt.Sprintf("Source service config: ClientTimeout cannot be 0. ClientTimeout: %v", badConfig.ClientTimeout)
			if expected != err.Error() {
				t.Errorf("expected error to be: '%v', got: '%v'", expected, err.Error())
			}
		})
	})
}
