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
│   |   └── main.go                # Main producer application
│   └── consume/                    # Consumes events from Kafka and writes to Postgres
│   |   └── main.go                # Main consumer application
├── internal/
│   ├── config/                     # Configuration for Kafka and Postgres
│   ├── consumer/                   # Functions to write event data to Postgres
│   ├── model/                      # Event and data models
│   └── producer/                   # Functions to generate random events
```

### Subdirectories
- **cmd/produce/**: CLI tool to generate and send random `User` and `Order` events to Kafka.
  - **kafka_topic_utils.go**: Utilities for Kafka topic management including topic existence checks and creation with readiness verification.
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

### Kafka Topic Management

The `cmd/produce/kafka_topic_utils.go` file provides utilities for managing Kafka topics:

#### Functions

##### `topicExists(broker string, topic string) bool`
- Checks if a Kafka topic exists by reading partition metadata

##### `createTopic(broker string, topic string, partitions int) error`
- Creates a new Kafka topic with specified number of partitions
- Automatically waits for the topic to be ready for writes using `waitForTopicReady()`
- Returns an error if topic creation or its readiness check fails

##### `waitForTopicReady(broker string, topic string, maxAttempts int) error`
- Waits for a newly created topic to be ready for write operations
- Uses a test write approach with a configurable test message key (from config)
- Implements retry logic with timeout to handle topic creation delays

#### Usage Example

```go
// Check if topic exists
if !topicExists(config.KafkaBroker, config.UsersTopic) {
    // Create topic if it doesn't exist
    err := createTopic(config.KafkaBroker, config.UsersTopic, config.UsersNumPartitions)
    if err != nil {
        panic(err)
    }
}
```

#### Key Features

- **Automatic Topic Creation**: Creates topics if they don't exist before producing events
- **Readiness Verification**: Ensures topics are fully ready for writes before proceeding
- **Retry Logic**: Handles transient failures during topic creation and readiness checks
- **Timeout Protection**: Uses context timeouts to prevent hanging operations
- **Test Message Integration**: Uses configurable test message keys to avoid polluting topics with hardcoded test data