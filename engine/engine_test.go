package engine_test

import (
	"collection-engine/engine"
	"collection-engine/test_utils"
	"fmt"
	"testing"
	"time"
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

		for range ce.ProcessingService.ProcessedMessages {
			fmt.Println("processed")
		}
	})

}
