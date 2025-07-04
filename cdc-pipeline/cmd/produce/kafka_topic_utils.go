package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func topicExists(broker string, topic string) bool {

	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to Kafka broker at %s: %v\n", broker, err))
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(fmt.Sprintf("failed to read partitions: %v\n", err))
	}

	topics := make(map[string]bool)
	for _, p := range partitions {
		topics[p.Topic] = true
	}

	// Check if the topic exists in the map
	return topics[topic]
}

func createTopic(broker string, topic string, partitions int) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka broker at %s: %v", broker, err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	fmt.Printf("Topic '%s' created with %d partitions\n", topic, partitions)

	return nil
}

func writeWithRetry(writer *kafka.Writer, topic string, msgBatch []kafka.Message, maxAttempts int, backOffTimeout int) {
	writeSuccess := false
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = writer.WriteMessages(context.Background(), msgBatch...)
		if err != nil {
			fmt.Printf("Writing to '%s' topic failed on attempt %d/%d. Waiting for %d seconds...\n", topic, attempt+1, maxAttempts, backOffTimeout)
			time.Sleep(2 * time.Second)
		} else {
			writeSuccess = true
			break
		}
	}
	// exit if failure after max. attempt
	if !writeSuccess {
		fmt.Printf("❌ Failed to write Order events after maximum attempts: %v\n", err)
		os.Exit(1)
	}
}
