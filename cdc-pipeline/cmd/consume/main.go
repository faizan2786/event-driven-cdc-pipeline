package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/config"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/consumer"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	"github.com/segmentio/kafka-go"
)

const TIME_OUT time.Duration = 5 * time.Second // in secs

func main() {
	var wg sync.WaitGroup

	// consume user events in parallel (as a consumer group)
	for i := 0; i < config.USERS_NUM_PARTITIONS; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeUserEvents()
		}()
	}

	// consume order events in parallel
	for i := 0; i < config.ORDERS_NUM_PARTITIONS; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeOrderEvents()
		}()
	}

	wg.Wait() // Wait for all goroutines to finish
}

// consume user events - blocks until new message arrives or time out reached
func consumeUserEvents() {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.KAFKA_BROKER},
		Topic:   config.USERS_TOPIC,
		GroupID: "go-consumer-group-users",
	})
	defer reader.Close()

	// get a context to cancel blocking after a time out
	// (i.e. cancel reading when no new messages are available after the timeout)

	ctx, cancel := context.WithTimeout(context.Background(), TIME_OUT)
	defer cancel()

	db, err := consumer.ConnectToDB()
	if err != nil {
		msg := fmt.Sprintf("Failed to Connect to the DB:\n%v\n", err)
		panic(msg)
	}
	defer db.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			fmt.Printf("Error while reading User events: %v\n", err)
			break
		}
		fmt.Printf("Partition: %v, Offset: %v\nKey: %s, Message: %s\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		// de-serialise event and put it into DB...
		var u model.UserEvent
		err = json.Unmarshal(msg.Value, &u)
		if err != nil {
			fmt.Printf("Error while unmarshaling User event: %v\n", err)
			break
		}

		success := consumer.AddUserEventToDB(db, u)
		if !success {
			break
		}
		// commit the offset
		reader.CommitMessages(ctx, msg)
	}
}

// consume user events - blocks until new message arrives or time out reached
func consumeOrderEvents() {

	db, err := consumer.ConnectToDB()
	if err != nil {
		msg := fmt.Sprintf("Failed to connect to the DB:\n%v\n", err)
		panic(msg)
	}
	defer db.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.KAFKA_BROKER},
		Topic:   config.ORDERS_TOPIC,
		GroupID: "go-consumer-group-orders",
	})

	ctx, cancel := context.WithTimeout(context.Background(), TIME_OUT)
	defer cancel()

	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Printf("Error while reading Order events from Kafka: %v\n", err)
			break
		}

		fmt.Printf("Partition: %v, Offset: %v\nKey: %s, Message: %s\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		// de-serialise event and put it into DB...
		var event model.OrderEvent
		json.Unmarshal(msg.Value, &event)
		success := consumer.AddOrderEventToDB(db, event)

		if !success {
			break
		}
		// commit the offset
		r.CommitMessages(ctx, msg)
	}
}
