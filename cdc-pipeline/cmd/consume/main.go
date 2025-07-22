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
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/kafkautils"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	"github.com/segmentio/kafka-go"
)

const (
	initialBackOff          int           = 1                // number of seconds to wait before the first retry attempt
	groupMaxAttempts        int           = 6                // max. attempts to retry for group status check
	processEventMaxAttempts int           = 3                // max. attempts to retry for processing message
	consumptionTimeOut      time.Duration = 20 * time.Second // total time to keep the consumer alive (in seconds)
)

type eventHandler func(msg kafka.Message, db *sql.DB) bool

type consumerConfig struct {
	topic         string
	numPartitions int
	groupId       string
	handler       eventHandler
}

func main() {

	consumerConfigs := []consumerConfig{
		{
			topic:         config.UsersTopic,
			numPartitions: config.UsersNumPartitions,
			groupId:       config.UsersConsumerGroupId,
			handler:       handleUserEvent,
		},
		{
			topic:         config.OrdersTopic,
			numPartitions: config.OrdersNumPartitions,
			groupId:       config.OrdersConsumerGroupId,
			handler:       handleOrderEvent,
		},
	}

	var wg sync.WaitGroup
	for i := range consumerConfigs {
		wg.Add(1)
		go func(c *consumerConfig) {
			defer wg.Done()
			consumeEvents(c)
		}(&consumerConfigs[i])
	}

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

// consume events - blocks until new message arrives or time out reached
func consumeEvents(c *consumerConfig) {

	// create the topic if it doesn't exist
	if !kafkautils.TopicExists(c.topic, config.KafkaBrokers...) {
		fmt.Printf("Topic '%s' not found. Creating the topic...\n", c.topic)
		err := kafkautils.CreateTopic(config.KafkaBrokers[0], c.topic, c.numPartitions, config.KafkaReplicationFactor)
		if err != nil {
			panic(err)
		}
	}

	// create a new reader (this will cause rebalancing of partition in Kafka)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: config.KafkaBrokers,
		Topic:   c.topic,
		GroupID: c.groupId,
	})

	// check consumer group state and wait for it to be ready before start reading
	err := kafkautils.WaitForGroupReady(config.KafkaBrokers, c.groupId, groupMaxAttempts, initialBackOff)
	if err != nil {
		panic(err)
	}

	db, err := consumer.ConnectToDB()
	if err != nil {
		msg := fmt.Sprintf("Failed to connect to the DB:\n%v\n", err)
		panic(msg)
	}
	defer db.Close()

	// create a channel per partition
	chPerPartition := make(map[int]chan kafka.Message)
	for i := range c.numPartitions {
		chPerPartition[i] = make(chan kafka.Message, 100) // buffered channel
	}

	ctx, cancel := context.WithTimeout(context.Background(), consumptionTimeOut)
	defer cancel()

	// dispatch message processing to workers (one per partition)
	fmt.Printf("Starting a worker per partition for '%s' topic...\n", c.topic)
	wg := &sync.WaitGroup{}
	for i := range c.numPartitions {
		wg.Add(1)
		startWorker(chPerPartition[i], r, db, c.handler, wg, ctx)
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
	for i := range c.numPartitions {
		close(chPerPartition[i])
	}
	wg.Wait() // Wait for all workers to finish
}

func startWorker(ch <-chan kafka.Message, r *kafka.Reader, db *sql.DB, dbHandler eventHandler, wg *sync.WaitGroup, ctx context.Context) {
	go func() {
		defer wg.Done()
		for msg := range ch {
			fmt.Printf("Topic: %s, Partition: %v, Offset: %v\nKey: %s, Message: %s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

			var success bool

			// process message with a backoff retry strategy...
			for i := 0; i < processEventMaxAttempts; i++ {
				success = dbHandler(msg, db)
				if success {
					break
				}
				delay := initialBackOff << i
				fmt.Printf("[Attempt %d/%d] DB event handler failed for '%s', trying again in %d seconds...\n", i+1, processEventMaxAttempts, msg.Topic, delay)
				time.Sleep(time.Duration(delay) * time.Second)
			}

			// stop consuming further if the current message was failed to process
			if !success {
				break
			}

			// commit the offset
			r.CommitMessages(ctx, msg)
		}
	}()
}
