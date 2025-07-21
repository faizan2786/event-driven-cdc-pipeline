package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/config"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/consumer"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	"github.com/segmentio/kafka-go"
)

const timeOut time.Duration = 10 * time.Second // in secs

func main() {
	var wg sync.WaitGroup

	// start two consumer routines (one per each topic)
	wg.Add(1)
	go func() {
		consumeUserEvents()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		consumeOrderEvents()
		wg.Done()
	}()

	wg.Wait()
}

func handleUserEvent(msg kafka.Message, db *sql.DB) bool {
	// de-serialise event and put it into DB...
	var event model.UserEvent
	json.Unmarshal(msg.Value, &event)
	return consumer.AddUserEventToDB(db, event)
}

func handleOrderEvent(msg kafka.Message, db *sql.DB) bool {
	// de-serialise event and put it into DB...
	var event model.OrderEvent
	json.Unmarshal(msg.Value, &event)
	return consumer.AddOrderEventToDB(db, event)
}

// consume user events - blocks until new message arrives or time out reached
func consumeUserEvents() {

	// create a channel per partition
	chPerPartition := make(map[int]chan kafka.Message)
	for i := range config.UsersNumPartitions {
		chPerPartition[i] = make(chan kafka.Message, 100) // buffered channel
	}

	db, err := consumer.ConnectToDB()
	if err != nil {
		msg := fmt.Sprintf("Failed to connect to the DB:\n%v\n", err)
		panic(msg)
	}
	defer db.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: config.KafkaBrokers,
		Topic:   config.UsersTopic,
		GroupID: "go-consumer-group-users",
	})

	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	// start workers (one per partition)
	fmt.Printf("Starting a worker per partition for '%s' topic...\n", config.UsersTopic)
	wg := &sync.WaitGroup{}
	for i := range config.UsersNumPartitions {
		wg.Add(1)
		startWorker(chPerPartition[i], r, db, handleUserEvent, wg, ctx)
	}

	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Printf("Error while reading Users events from Kafka: %v\n", err)
			break
		}

		// dispatch the message to channel based on its partition
		chPerPartition[msg.Partition] <- msg
	}

	// closing worker channels
	for i := range config.UsersNumPartitions {
		close(chPerPartition[i])
	}
	wg.Wait() // Wait for all workers to finish
}

// consume user events - blocks until new message arrives or time out reached
func consumeOrderEvents() {

	// create a channel per partition
	chPerPartition := make(map[int]chan kafka.Message)
	for i := range config.OrdersNumPartitions {
		chPerPartition[i] = make(chan kafka.Message, 100) // buffered channel
	}

	db, err := consumer.ConnectToDB()
	if err != nil {
		msg := fmt.Sprintf("Failed to connect to the DB:\n%v\n", err)
		panic(msg)
	}
	defer db.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: config.KafkaBrokers,
		Topic:   config.OrdersTopic,
		GroupID: "go-consumer-group-orders",
	})

	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	// start workers (one per partition)
	fmt.Printf("Starting a worker per partition for '%s' topic...\n", config.OrdersTopic)
	wg := &sync.WaitGroup{}
	for i := range config.OrdersNumPartitions {
		wg.Add(1)
		startWorker(chPerPartition[i], r, db, handleOrderEvent, wg, ctx)
	}

	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Printf("Error while reading Order events from Kafka: %v\n", err)
			break
		}

		// dispatch the message to channel based on its partition
		chPerPartition[msg.Partition] <- msg
	}

	// closing worker channels
	for i := range config.OrdersNumPartitions {
		close(chPerPartition[i])
	}
	wg.Wait() // Wait for all workers to finish
}

func startWorker(ch <-chan kafka.Message, r *kafka.Reader, db *sql.DB, msgHandler func(msg kafka.Message, db *sql.DB) bool,
	wg *sync.WaitGroup, ctx context.Context) {
	go func() {
		defer wg.Done()
		for msg := range ch {
			fmt.Printf("Topic: %s, Partition: %v, Offset: %v\nKey: %s, Message: %s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			success := msgHandler(msg, db)
			if !success {
				break
			}
			// commit the offset
			r.CommitMessages(ctx, msg)
		}
	}()
}
