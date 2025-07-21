package kafkautils

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func TopicExists(topic string, brokers ...string) bool {

	kafkaClient := &kafka.Client{
		Addr:    kafka.TCP(brokers...),
		Timeout: 5 * time.Second,
	}

	// Get cluster metadata
	clusterInfo, err := kafkaClient.Metadata(context.Background(), &kafka.MetadataRequest{})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to Kafka cluster: %v\n", err))
	}

	fmt.Printf("Kafka cluster controller found: %v\n", clusterInfo.Controller)

	topics := make(map[string]bool)
	for _, t := range clusterInfo.Topics {
		if !t.Internal {
			topics[t.Name] = true
		}
	}

	// Check if the topic exists in the map
	return topics[topic]
}

func CreateTopic(broker string, topic string, partitions int, replicationFactor int) error {
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
			ReplicationFactor: replicationFactor,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	fmt.Printf("Topic '%s' created with %d partitions\n", topic, partitions)

	return nil
}

func WriteWithRetry(writer *kafka.Writer, topic string, msgBatch []kafka.Message, maxAttempts int, backOffTimeout int) {
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
