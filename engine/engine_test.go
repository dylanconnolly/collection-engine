package engine_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dylanconnolly/collection-engine/engine"
	"github.com/dylanconnolly/collection-engine/test_utils"
	"github.com/google/go-cmp/cmp"
)

func TestEngineRun(t *testing.T) {
	t.Run("with successful responses", func(t *testing.T) {
		cfg := test_utils.BuildCollectionEngineConfig(10, 120, 5)

		sourceResponse := engine.MessageResponse{
			Results: test_utils.GenerateMockMessages(1),
			Cursor:  nil,
		}
		processingResponse := engine.ProcessedMessage{
			sourceResponse.Results[0],
			time.Now().UTC().String(),
		}

		ce := engine.NewCollectionEngine(cfg)
		sourceServer := test_utils.CreateTestServer(ce.SourceService, "/messages", sourceResponse, 200)
		defer sourceServer.Close()

		processingServer := test_utils.CreateTestServer(ce.ProcessingService, "/messages", processingResponse, 200)
		defer processingServer.Close()
		cancel := make(chan bool)
		ce.Run(cancel)

		count := 0
		for range ce.ProcessingService.ProcessedMessages {
			fmt.Println("added to processed")
			count++
			fmt.Println("processed: ", count)
			if count == 120 {
				cancel <- true
				cancel <- true
			}
			// time.Sleep(1 * time.Second)
		}

		time.Sleep(3 * time.Second)

		fmt.Println("message channel length: ", len(ce.SourceService.Messages))
		fmt.Println("processed messages channel length: ", len(ce.ProcessingService.ProcessedMessages))

		// for r := range ce.ProcessingService.ProcessedMessages {
		// 	fmt.Println(r)
		// }
	})

	t.Run("errors during processing should be sent to retry queue", func(t *testing.T) {
		cfg := test_utils.BuildCollectionEngineConfig(10, 120, 5)

		sourceResponse := engine.MessageResponse{
			Results: test_utils.GenerateMockMessages(1),
			Cursor:  nil,
		}
		processingResponse := engine.ProcessedMessage{
			sourceResponse.Results[0],
			time.Now().UTC().String(),
		}

		ce := engine.NewCollectionEngine(cfg)
		sourceServer := test_utils.CreateTestServer(ce.SourceService, "/messages", sourceResponse, 200)
		defer sourceServer.Close()

		processingServer := test_utils.CreateTestServer(ce.ProcessingService, "/messages", processingResponse, 500)
		defer processingServer.Close()
		cancel := make(chan bool)
		ce.Run(cancel)

		time.Sleep(2)
		cancel <- true
		cancel <- true

		// for range ce.ProcessingService.ProcessedMessages {
		// 	fmt.Println("processed")
		// }
	})

}

func TestMessageStruct(t *testing.T) {
	expected := `{
		"id": "924c8cfbd9f94155985bf262cf2c3c67",
		"source": "MessagingSystem",
		"title": "Where are my pants?",
		"creation_date": "2030-08-24T17:16:52.228009",
		"message": "Erlang is known...",
		"tags": [
		"no",
		"collection",
		"building",
		"seeing"
		],
		"author": "Dominic Mccormick"
		}`
	msg := engine.Message{
		ID:           "924c8cfbd9f94155985bf262cf2c3c67",
		Source:       "MessagingSystem",
		Title:        "Where are my pants?",
		CreationDate: "2030-08-24T17:16:52.228009",
		Message:      "Erlang is known...",
		Tags:         []string{"no", "collection", "building", "seeing"},
		Author:       "Dominic Mccormick",
	}

	httpResponse := []byte(expected)

	var uMsg engine.Message
	err := json.Unmarshal(httpResponse, &uMsg)
	if err != nil {
		t.Error("should not get error unmarshalling expected message response")
	}

	httpBody, err := json.Marshal(&msg)
	if err != nil {
		t.Error("error marshalling message into json body")
	}

	if cmp.Equal(expected, string(httpBody)) {
		t.Errorf("marshalling message into request body, expected:\n%s\ngot:\n%s\n", expected, string(httpBody))
	}
}

func TestProcessedMessageStruct(t *testing.T) {
	expected := `{
		"id": "924c8cfbd9f94155985bf262cf2c3c67",
		"source": "MessagingSystem",
		"title": "Where are my pants?",
		"creation_date": "2030-08-24T17:16:52.228009",
		"message": "Erlang is known...",
		"tags": [
		"no",
		"collection",
		"building",
		"seeing"
		],
		"author": "Dominic Mccormick",
		"processing_date": "2030-08-24T17:16:52.228009"
		}`
	pmsg := engine.ProcessedMessage{
		engine.Message{
			ID:           "924c8cfbd9f94155985bf262cf2c3c67",
			Source:       "MessagingSystem",
			Title:        "Where are my pants?",
			CreationDate: "2030-08-24T17:16:52.228009",
			Message:      "Erlang is known...",
			Tags:         []string{"no", "collection", "building", "seeing"},
			Author:       "Dominic Mccormick",
		},
		"2030-08-24T17:16:52.228009",
	}

	httpResponse := []byte(expected)

	var uMsg engine.ProcessedMessage
	err := json.Unmarshal(httpResponse, &uMsg)
	if err != nil {
		t.Errorf("should not get error unmarshalling expected processed response, err: %v", err)
	}

	httpBody, err := json.Marshal(&pmsg)
	if err != nil {
		t.Error("error marshalling message into json body")
	}

	if cmp.Equal(expected, string(httpBody)) {
		t.Errorf("marshalling message into request body, expected:\n%s\ngot:\n%s\n", expected, string(httpBody))
	}

}
