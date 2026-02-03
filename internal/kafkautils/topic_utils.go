package kafkautils

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/internal/logger"
	"github.com/segmentio/kafka-go"
)

func TopicExists(topic string, brokers ...string) (bool, error) {

	kafkaClient := &kafka.Client{
		Addr:    kafka.TCP(brokers...),
		Timeout: 5 * time.Second,
	}

	// Get cluster metadata
	clusterInfo, err := kafkaClient.Metadata(context.Background(), &kafka.MetadataRequest{})
	if err != nil {
		return false, fmt.Errorf("failed to connect to Kafka cluster: %w", err)
	}

	logger.DebugLogger.Printf("Kafka cluster controller found: %v\n", clusterInfo.Controller)

	topics := make(map[string]struct{}) // a set of topic names
	for _, t := range clusterInfo.Topics {
		if !t.Internal {
			topics[t.Name] = struct{}{}
		}
	}

	// Check if the given topic exists in the map
	_, ok := topics[topic]
	return ok, nil
}

func CreateTopic(broker string, topic string, partitions int, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka broker at %s: %v", broker, err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get the kafka controller: %w", err)
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("failed to connect to the controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfig := kafka.TopicConfig{

		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}

	err = controllerConn.CreateTopics(topicConfig)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	logger.InfoLogger.Printf("Topic '%s' created with %d partitions\n", topic, partitions)

	return nil
}

func WriteWithRetry(writer *kafka.Writer, topic string, msgBatch []kafka.Message, maxAttempts int, backOffTimeout int) error {
	writeSuccess := false
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = writer.WriteMessages(context.Background(), msgBatch...)
		if err != nil {
			logger.DebugLogger.Printf("Writing to '%s' topic failed on attempt %d/%d. Waiting for %d seconds...\n", topic, attempt+1, maxAttempts, backOffTimeout)
			time.Sleep(2 * time.Second)
		} else {
			writeSuccess = true
			break
		}
	}

	// return error if failed
	if !writeSuccess {
		return fmt.Errorf("failed to write to the topic after maximum attempt: %w", err)
	}
	return nil
}
