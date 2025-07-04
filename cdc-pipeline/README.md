# cdc-pipeline Go Module


This directory contains the application code for the Event Driven CDC Pipeline project.
Currently, it provides commands to generate (produce) and consume `User` and `Order` events using Kafka and PostgreSQL.

## Prerequisites
- Docker and Docker Compose
- Go 1.23+ (recommended: Go 1.24+)

## Directory Structure

```
cdc-pipeline/
├── cmd/                            # CLI entrypoints
│   ├── produce/                    # Produces random events to Kafka
│   |   ├── event_client.go         # A test program to generate events and inspect in json format
│   |   ├── kafka_topic_utils.go    # Kafka topic management utilities
│   |   └── main.go                 # Main producer application
│   └── consume/                    # Consumes events from Kafka and writes to Postgres
│   |   └── main.go                 # Main consumer application
├── internal/
│   ├── config/                     # Configuration for Kafka and Postgres
│   ├── consumer/                   # Functions to write event data to Postgres
│   ├── model/                      # Event and data models
│   └── producer/                   # Functions to generate random events
```

### Subdirectories
- **cmd/produce/**: CLI tool to generate and send random `User` and `Order` events to Kafka.
  - **kafka_topic_utils.go**: Utilities for Kafka topic management including topic existence checks, topic creation and writes with retry.
- **cmd/consume/**: CLI tool to consume `User` and `Order` events from Kafka and write them to Postgres.
- **internal/producer/**: Functions to generate random `User` and `Order` events.
- **internal/consumer/**: Functions to read `User` and `Order` event data and write them to Postgres.
- **internal/config/**: Configuration constants for Kafka and Postgres.

## Running Tests

To run all unit tests in the `internal` directory:

```sh
cd cdc-pipeline
go test ./internal/...
```

## Running Commands

### Produce Events
Build and run the producer to generate and send events to Kafka:
```sh
go run ./cmd/produce
```

### Consume Events
Build and run the consumer to read events from Kafka and write to Postgres:
```sh
go run ./cmd/consume
```

---

Ensure your services (Kafka, Postgres) are running via Docker Compose before running the Go commands. 


### Kafka Topic Management & Write Retry Logic

The `cmd/produce/kafka_topic_utils.go` file provides utilities for managing Kafka topics and robust message writing:

#### Functions

- **`topicExists(broker string, topic string) bool`**
  - Checks if a Kafka topic exists by reading partition metadata.

- **`createTopic(broker string, topic string, partitions int) error`**
  - Creates a new Kafka topic with the specified number of partitions.

- **`writeWithRetry(writer *kafka.Writer, topic string, msgBatch []kafka.Message, maxAttempts int, backOffTimeout int)`**
  - Writes a batch of messages to Kafka with retry logic.
  - Retries up to `maxAttempts` if the write fails, waiting `backOffTimeout` seconds between attempts.
  - Exits the program if all attempts fail.

#### Usage Example (Write with Retry on First Batch)

When producing events, the first batch for each topic is written using retry logic to handle cases where the topic may not be immediately ready for writes (e.g., the topic was just created by the producer):

```go
for i := 0; i < numBatches; i++ {
    // ... prepare msgBatch ...
    if i == 0 {
        writeWithRetry(writer, config.UsersTopic, msgBatch, maxAttempts, backOffTime)
    } else {
        // Subsequent batches write directly without retries
        err := writer.WriteMessages(context.Background(), msgBatch...)
    }
}
```
This pattern is used for both User and Order event producers.
#### Key Features

- **Automatic Topic Creation**: Creates topics if they don't exist before producing events.
- **Write Retry on First Batch**: Ensures the first batch of messages is reliably written, even if the topic is not immediately ready.
- **Robust Error Handling**: Exits on repeated write failures to avoid silent data loss.