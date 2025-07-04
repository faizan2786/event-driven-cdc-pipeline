package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/config"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/producer"
	"github.com/segmentio/kafka-go"
)

const (
	userBatchSize  int = 7
	orderBatchSize int = 10
)

func main() {
	userIds := produceUserEvents(userBatchSize, 1)
	produceOrderEvents(userIds, orderBatchSize, 1)
}

// returns list of User ids (to be used for order events)
func produceUserEvents(batchSize int, numBatches int) []model.UUID {

	// create the topic if it doesn't exist
	if !topicExists(config.KafkaBroker, config.UsersTopic) {
		err := createTopic(config.KafkaBroker, config.UsersTopic, config.UsersNumPartitions)
		fmt.Printf("Topic '%s' not found. Creating the topic...\n", config.UsersTopic)
		if err != nil {
			panic(err)
		}
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(config.KafkaBroker),
		Topic:     config.UsersTopic,
		Balancer:  &kafka.Hash{}, // partition by the Key in the message
		BatchSize: batchSize,
	}
	defer writer.Close()

	var myUserIDs []model.UUID

	for i := 0; i < numBatches; i++ {

		userEvents := producer.GenerateRandomUserEvents(batchSize)

		// prepare the kafka message batch
		var msgBatch []kafka.Message
		for _, e := range userEvents {

			// store the user ids
			if e.Type == model.CREATE {
				myUserIDs = append(myUserIDs, e.UserId)
			}

			jsonBytes, _ := json.Marshal(e)
			msg := kafka.Message{
				Key:   []byte(e.UserId),
				Value: jsonBytes,
			}
			msgBatch = append(msgBatch, msg)
		}

		err := writer.WriteMessages(context.Background(), msgBatch...)

		if err != nil {
			fmt.Printf("❌ Failed to write User events: %v\n", err)
			os.Exit(1)

		} else {
			fmt.Printf("✅ Sent a batch of %d User events\n", len(msgBatch))
		}
	}
	fmt.Println("Number of unique users created: ", len(myUserIDs))
	return myUserIDs
}

func produceOrderEvents(userIds []model.UUID, batchSize int, numBatches int) {

	if !topicExists(config.KafkaBroker, config.OrdersTopic) {
		fmt.Printf("Topic '%s' not found. Creating the topic...\n", config.OrdersTopic)
		err := createTopic(config.KafkaBroker, config.OrdersTopic, config.OrdersNumPartitions)
		if err != nil {
			panic(err)
		}
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(config.KafkaBroker),
		Topic:     config.OrdersTopic,
		Balancer:  &kafka.Hash{}, // partition by the Key in the message
		BatchSize: batchSize,
	}
	defer writer.Close()

	var myUserIDs map[model.UUID]bool = make(map[model.UUID]bool)

	for i := 0; i < numBatches; i++ {

		orderEvents := producer.GenerateRandomOrderEvents(batchSize, userIds)

		// prepare the kafka message batch
		var msgBatch []kafka.Message
		for _, e := range orderEvents {

			if e.Type == model.CREATE {
				myUserIDs[e.UserId] = true
			}

			jsonBytes, _ := json.Marshal(e)
			msg := kafka.Message{
				Key:   []byte(e.OrderId),
				Value: jsonBytes,
			}
			msgBatch = append(msgBatch, msg)
		}

		err := writer.WriteMessages(context.Background(), msgBatch...)

		if err != nil {
			fmt.Printf("❌ Failed to write Order events: %v\n", err)
			os.Exit(1)

		} else {
			fmt.Printf("✅ Sent a batch of %d Order events\n", len(msgBatch))
		}

		fmt.Println("Number of unique users used for new orders: ", len(myUserIDs))
	}
}
