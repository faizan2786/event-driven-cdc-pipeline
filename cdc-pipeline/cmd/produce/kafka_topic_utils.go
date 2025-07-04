package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/config"
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

	return waitForTopicReady(broker, topic, 5)
}

func waitForTopicReady(broker string, topic string, maxAttempts int) error {
	for i := 0; i < maxAttempts; i++ {
		// Create a test writer
		writer := &kafka.Writer{
			Addr:      kafka.TCP(broker),
			Topic:     topic,
			Balancer:  &kafka.Hash{},
			BatchSize: 1,
		}

		// Try to write a test message
		testMsg := kafka.Message{
			Key:   []byte(config.TestMsgKey),
			Value: []byte("waitForTopicReady test"),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // use timeout incase write message blocks for some reason
		err := writer.WriteMessages(ctx, testMsg)
		cancel()

		writer.Close()

		if err == nil {
			fmt.Printf("Topic '%s' is ready for writes\n", topic)
			return nil
		}

		fmt.Printf("Topic '%s' not ready yet, waiting for 2 seconds... (attempt %d/%d): %v\n", topic, i+1, maxAttempts, err)
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("topic '%s' not ready for writes after %d attempts", topic, maxAttempts)
}
