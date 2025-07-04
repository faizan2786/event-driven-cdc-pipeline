package main

import (
	"fmt"
	"net"
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

// sleep - seconds to wait after topic creation.
// Allowing some time to finish off topic creation
func createTopic(broker string, topic string, partitions int, sleep int) error {
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

	time.Sleep(time.Duration(sleep) * time.Second)

	return nil
}
