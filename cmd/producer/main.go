package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/faizan2786/event-driven-cdc-pipeline/internal/config"
	"github.com/faizan2786/event-driven-cdc-pipeline/internal/eventgenerator"
	"github.com/faizan2786/event-driven-cdc-pipeline/internal/kafkautils"
	"github.com/faizan2786/event-driven-cdc-pipeline/internal/logger"
	"github.com/faizan2786/event-driven-cdc-pipeline/internal/model"
	"github.com/segmentio/kafka-go"
)

const (
	userBatchSize   int = 4
	orderBatchSize  int = 4
	maxAttempts     int = 5
	backOffInterval int = 2 // number of seconds to wait between retry attempts
)

func main() {
	userIds, err := produceUserEvents(userBatchSize, 1)
	if err != nil {
		logger.ErrorLogger.Printf("produceUserEvents: %v\n", err)
		return
	}

	err = produceOrderEvents(userIds, orderBatchSize, 1)
	if err != nil {
		logger.ErrorLogger.Printf("produceOrderEvents: %v\n", err)
		return
	}
}

// returns list of User ids (to be used for order events)
func produceUserEvents(batchSize int, numBatches int) ([]model.UUID, error) {

	topicExists, err := kafkautils.TopicExists(config.UsersTopic, config.KafkaBrokers...)
	if err != nil {
		return nil, fmt.Errorf("TopicExists() = %w", err)
	}

	// create the topic if it doesn't exist
	if !topicExists {
		logger.InfoLogger.Printf("Topic '%s' not found. Creating the topic...\n", config.UsersTopic)
		err := kafkautils.CreateTopic(config.KafkaBrokers[0], config.UsersTopic, config.UsersNumPartitions, config.KafkaReplicationFactor)
		if err != nil {
			return nil, fmt.Errorf("CreateTopic() =  %w", err)
		}
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(config.KafkaBrokers...),
		Topic:     config.UsersTopic,
		Balancer:  &kafka.Hash{}, // partition by the Key in the message
		BatchSize: batchSize,
	}
	defer writer.Close()

	var myUserIDs []model.UUID

	// generate batch of events and send it to Kafka
	for i := 0; i < numBatches; i++ {

		userEvents := eventgenerator.GenerateRandomUserEvents(batchSize)

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

		// write with retry for the first batch (in case topic is not ready to write yet)
		if i == 0 {
			err := kafkautils.WriteWithRetry(writer, config.UsersTopic, msgBatch, maxAttempts, backOffInterval)
			if err != nil {
				return nil, fmt.Errorf("WriteWithRetry() = %w", err)
			}
		} else {
			err := writer.WriteMessages(context.Background(), msgBatch...)
			if err != nil {
				return nil, fmt.Errorf("WriteMessages() = failed to write User events: %w", err)
			}
		}
		logger.InfoLogger.Printf("✅ Sent a batch of %d User events\n", len(msgBatch))
	}

	logger.DebugLogger.Println("Number of users created: ", len(myUserIDs))
	return myUserIDs, nil
}

func produceOrderEvents(userIds []model.UUID, batchSize int, numBatches int) error {

	topicExists, err := kafkautils.TopicExists(config.OrdersTopic, config.KafkaBrokers...)
	if err != nil {
		return fmt.Errorf("TopicExists() = %w", err)
	}

	// create the topic if it doesn't exist
	if !topicExists {
		logger.InfoLogger.Printf("Topic '%s' not found. Creating the topic...\n", config.OrdersTopic)
		err := kafkautils.CreateTopic(config.KafkaBrokers[0], config.OrdersTopic, config.OrdersNumPartitions, config.KafkaReplicationFactor)
		if err != nil {
			return fmt.Errorf("CreateTopic() =  %w", err)
		}
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(config.KafkaBrokers...),
		Topic:     config.OrdersTopic,
		Balancer:  &kafka.Hash{}, // partition by the Key in the message
		BatchSize: batchSize,
	}
	defer writer.Close()

	numOrders := 0
	var myUserIDs = make(map[model.UUID]struct{}) // a set of user IDs

	// generate batch of events and send it to Kafka
	for i := 0; i < numBatches; i++ {

		orderEvents := eventgenerator.GenerateRandomOrderEvents(batchSize, userIds)

		// prepare the kafka message batch
		var msgBatch []kafka.Message
		for _, e := range orderEvents {

			if e.Type == model.CREATE {
				numOrders += 1
				myUserIDs[e.UserId] = struct{}{}
			}

			jsonBytes, _ := json.Marshal(e)
			msg := kafka.Message{
				Key:   []byte(e.OrderId),
				Value: jsonBytes,
			}
			msgBatch = append(msgBatch, msg)
		}

		// write with retry for the first batch (in case topic is not ready to write yet)
		if i == 0 {
			err := kafkautils.WriteWithRetry(writer, config.OrdersTopic, msgBatch, maxAttempts, backOffInterval)
			if err != nil {
				return fmt.Errorf("WriteWithRetry() = %w", err)
			}
		} else {
			err := writer.WriteMessages(context.Background(), msgBatch...)
			if err != nil {
				return fmt.Errorf("WriteMessages() = failed to write User events: %w", err)
			}
		}

		logger.InfoLogger.Printf("✅ Sent a batch of %d Order events\n", len(msgBatch))
	}

	logger.DebugLogger.Println("Number of orders created: ", numOrders)
	logger.DebugLogger.Println("Number of unique users used for new orders: ", len(myUserIDs))
	return nil
}
