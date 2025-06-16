package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/faizan2786/system-design/cdc-pipeline/internal/config"
	"github.com/faizan2786/system-design/cdc-pipeline/internal/model"
	"github.com/faizan2786/system-design/cdc-pipeline/internal/producer"
	"github.com/segmentio/kafka-go"
)

const USER_BATCH_SIZE int = 7
const ORDER_BATCH_SIZE int = 20

func main() {
	userIds := produceUserEvents(USER_BATCH_SIZE, 1)
	produceOrderEvents(userIds, ORDER_BATCH_SIZE, 1)
}

// returns list of User ids (to be used for order events)
func produceUserEvents(batchSize int, numBatches int) []model.UUID {

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(config.KAFKA_BROKER),
		Topic:                  config.USERS_TOPIC,
		Balancer:               &kafka.Hash{}, // partition by the Key in the message
		BatchSize:              batchSize,
		AllowAutoTopicCreation: true,
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

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(config.KAFKA_BROKER),
		Topic:                  config.ORDERS_TOPIC,
		Balancer:               &kafka.Hash{}, // partition by the Key in the message
		BatchSize:              batchSize,
		AllowAutoTopicCreation: true,
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
