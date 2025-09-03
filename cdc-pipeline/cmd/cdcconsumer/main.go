package main

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/config"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/logger"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/parser"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/sink"
	"github.com/segmentio/kafka-go"
)

func main() {
	// topics produced by Debezium
	cdcTopics := []string{
		config.DebeziumUsersTopic,
		config.DebeziumOrdersTopic,
	}

	// create cassandra client
	cs, err := sink.NewCassandraClient(config.CassandraHosts, config.CassandraKeyspace)
	if err != nil {
		logger.ErrorLogger.Printf("failed to init cassandra client: %v\n", err)
		os.Exit(1)
	}
	defer cs.Close()

	wg := sync.WaitGroup{}
	for _, t := range cdcTopics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			consumeTopic(topic, cs)
		}(t)
	}
	wg.Wait()
	logger.InfoLogger.Println("cdc-consumer stopped")
}

func consumeTopic(topic string, cqlClient *sink.CassandraClient) {

	// create a kafka reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: config.KafkaBrokers,
		Topic:   topic,
		GroupID: "cdc-cassandra-sink",
	})
	defer r.Close()

	logger.InfoLogger.Printf("starting consumer for topic=%s\n", topic)

	// setup retry variables for retries
	fetchRetry, commitRetry, maxRetries, retryBackOff := 0, 0, 3, 2 // retryBackOff in seconds
	timeOutSecs := 10

	// message consumption loop
	for {
		// parent context cancellation
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeOutSecs)*time.Second)
		msg, err := r.FetchMessage(ctx)
		cancel()

		// retry logic
		if err != nil {

			if err == context.DeadlineExceeded {
				logger.InfoLogger.Printf("No new messages received in last %d seconds for topic %s. Shutting down consumer...\n", timeOutSecs, topic)
				break
			}

			logger.ErrorLogger.Printf("error fetching message from %s: %v\n", topic, err)
			if fetchRetry == maxRetries {
				break
			}
			// small backoff then try fetching again
			time.Sleep(time.Duration(retryBackOff) * time.Second)
			fetchRetry += 1
			continue
		}
		fetchRetry = 0 // reset number of retries

		// parse Debezium events
		ev, err := parser.ParseDebeziumEvent(msg.Value)
		if err != nil {
			logger.ErrorLogger.Printf("message parsing error: topic=%s partition=%d offset=%d: %v\n", msg.Topic, msg.Partition, msg.Offset, err)
			break
		}

		// apply to Cassandra (idempotent due to processed_events table)
		if err := cqlClient.ApplyChange(topic, ev); err != nil {
			logger.ErrorLogger.Printf("apply change error: topic=%s partition=%d offset=%d: %v\n", msg.Topic, msg.Partition, msg.Offset, err)
			break
		}

		// commit kafka offset after successful persist
		if err := r.CommitMessages(context.Background(), msg); err != nil {
			logger.ErrorLogger.Printf("failed to commit offset: %v\n", err)
			if commitRetry == maxRetries {
				break
			}
			// small backoff then try committing again
			time.Sleep(time.Duration(retryBackOff) * time.Second)
			commitRetry += 1
			continue
		}
		commitRetry = 0
	}
}
