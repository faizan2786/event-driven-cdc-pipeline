package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/faizan2786/system-design/cdc-pipeline/internal/config"
	"github.com/segmentio/kafka-go"
)

const TIME_OUT int = 3000 // in mill secs

func main() {

	// consume user events in parallel
	var wg sync.WaitGroup
	for i := 0; i < config.USERS_NUM_PARTITIONS; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeUserEvents()
		}()
	}

	wg.Wait() // Wait for all goroutines to finish
}

// consume user events from given partition id and display on the console
// block until new messages or time out
func consumeUserEvents() {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.KAFKA_BROKER},
		Topic:   config.USERS_TOPIC,
		GroupID: "go-consumer-group",
	})
	defer reader.Close()

	// get a context to cancel blocking after a time out
	// (i.e. cancel reading when no new messages are available after the timeout)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TIME_OUT)*time.Millisecond)
	defer cancel()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			fmt.Printf("Error while reading User events: %v\n", err)
			break
		}
		fmt.Printf("Partition: %v, Offset: %v\nKey: %s, Message: %s\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		// commit the offset
		reader.CommitMessages(ctx, msg)
	}
}
