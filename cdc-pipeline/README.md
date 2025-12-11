# cdc-pipeline Go Module


This directory contains the main application code for the project.
Currently, it provides 3 commands/executables to generate (produce) business events and consume business (i.e. `User` and `Order` events) and change events (i.e. Debezium CDC events) to and from Kafka.

## Prerequisites
- Docker and Docker Compose
- Go 1.23+ (recommended: Go 1.24+)

## Directory Structure

```
cdc-pipeline/
├── cmd/                            # CLI entrypoints
│   ├── producer/                   # Produces random User and Order events to Kafka
│   │   ├── generate_events.go      # A test program to generate events and output them in json format
│   │   └── main.go                 # Main producer application
│   ├── consumer/                   # Consumes events from Kafka and writes to Postgres DB
│   │   └── main.go                 # Multi-topic consumer with worker pools and idle timeout
│   └── cdcconsumer/                # CDC consumer for Debezium change events
│       └── main.go                 # Consumes CDC events from Debezium kafka topics and syncs changes to Cassandra
├── internal/
│   ├── config/                     # Configuration constants and settings
│   │   ├── kafka_config.go         # Kafka config
│   │   ├── postgres_config.go      # Postgres config
│   │   └── cassandra_config.go     # Cassandra config
│   ├── eventgenerator/             # Event generator logic (and tests)
│   │   ├── order_event_generator.go
│   │   ├── order_event_generator_test.go
│   │   ├── user_event_generator.go
│   │   └── user_event_generator_test.go
│   ├── kafkautils/                 # Kafka utilities (topic and group management)
│   │   ├── group_utils.go
│   │   └── topic_utils.go
│   ├── logger/                     # Custom logging system
│   │   └── logger.go               # Three-level logger (INFO, DEBUG, ERROR)
│   ├── model/                      # Event and data models
│   │   ├── events.go               # Business event models
│   │   └── change_event.go         # CDC change event model
│   ├── parser/                     # Event parsing logic
│   │   ├── debezium_event_parser.go # Debezium CDC event parser
│   │   └── debezium_event_parser_test.go
│   └── sink/                       # Sink event logic for DBs (and tests)
│       ├── postgres_sink.go
│       ├── postgres_sink_test.go
│       ├── cassandra_sink.go       # Cassandra sink with correct CQL types
│       └── cassandra_sink_test.go
├── .golangci.yaml                  # config file for golangci linter
```

### Subdirectories
- **cmd/producer/**: CLI tool to generate and send random `User` and `Order` events to Kafka.
- **cmd/consumer/**: CLI tool with multi-topic consumer that uses worker pools per partition to consume events in parallel.
- **cmd/cdcconsumer/**: CDC consumer that processes Debezium change events and syncs them to Cassandra.
- **internal/config/**: Kafka, Postgres and Cassandra configuration.
- **internal/eventgenerator/**: Event generator logic and tests for `User` and `Order` events.
- **internal/kafkautils/**: Kafka utilities for topic and group management.
- **internal/logger/**: Custom three-level logging system (INFO, DEBUG, ERROR).
- **internal/model/**: Event and data models, including CDC change events.
- **internal/parser/**: Debezium change event parsing logic.
- **internal/sink/**: Sink event logic for Postgres and Cassandra DBs with proper CQL type handling.

## Running Tests

To run all unit tests in the `internal` directory:

```sh
cd cdc-pipeline
go test ./internal/...
```

## Key Features

- **3-Broker Kafka Cluster**: Configured with brokers `kafka1:9092`, `kafka2:9092`, `kafka3:9092` with replication factor of 3
- **Intelligent Consumer**: Multi-topic consumer with idle timeout that gracefully shuts down when no messages arrive
- **Worker Pool Architecture**: One worker per partition for parallel message processing
- **Automatic Topic Management**: Creates topics with proper partitioning if they don't exist
- **Retry Logic**: Exponential backoff for failed Kafka write operations
- **Modular Design**: Easy to extend with new event types and handlers
- **Custom Logging System**: Three-level logger (INFO, DEBUG, ERROR) with structured output
- **CDC Pipeline**: Comprehensive CDC pipeline for capturing and syncing primary database changes to the external system (i.e. Cassandra)
- **Schema Support**: Updated to support Cassandra orders table with `order_id` primary key

## Running Commands

### Produce Events
Build and run the producer to generate and send events to Kafka:
```sh
go run ./cmd/producer
```

### Consume Events
Build and run the consumer to read events from Kafka and write to Postgres:
```sh
go run ./cmd/consumer
```
The consumer will:
- Start workers for each partition of `users` and `orders` topics
- Process messages in parallel
- Gracefully exit when no messages arrive for the idle timeout period (10 seconds by default)
- Commit offsets after successful processing

### Consume CDC Events
Build and run the CDC consumer to process Debezium change events and sync to Cassandra:
```sh
go run ./cmd/cdcconsumer
```
The CDC consumer will:
- Process CDC events from `cdc.public.users` and `cdc.public.orders` topics
- Apply changes idempotently to Cassandra using correct CQL types
- Handle graceful shutdown with configurable timeout
- Retry failed operations with exponential backoff

---

Ensure your services (3-node Kafka cluster, Postgres) are running via Docker Compose before running the Go commands. 


### Kafka Utilities & Consumer Architecture

The `internal/kafkautils/` package provides robust utilities for Kafka operations:

#### Topic Management (`topic_utils.go`)

- **`TopicExists(topic string, brokers ...string) bool`**
  - Checks if a Kafka topic exists across the cluster.

- **`CreateTopic(broker string, topic string, partitions int, replicationFactor int) error`**
  - Creates a new Kafka topic with specified partitions and replication factor.

- **`WriteWithRetry(writer *kafka.Writer, topic string, msgBatch []kafka.Message, maxAttempts int, backOffTimeout int)`**
  - Writes messages with fixed backoff interval and retry logic.

#### Consumer Group Management (`group_utils.go`)

- **`WaitForGroupReady(brokers []string, groupID string, maxAttempts int, backOffStartTime int) error`**
  - Ensures consumer group is ready before starting consumption with exponential backoff and retry logic.

#### Consumer Architecture

The consumer uses a worker pool architecture for high throughput:

```go
type consumerConfig struct {
    Topic         string
    NumPartitions int
    GroupID       string
    Handler       eventHandler
}

type eventHandler func(msg kafka.Message, db *sql.DB) bool
```

**Features:**
- **Multi-topic support**: Single application handles multiple event types
- **Per-partition workers**: One goroutine per partition per topic for concurrent processing
- **Idle timeout**: Exits when no messages arrive for a configurable period
- **Retry logic**: Exponential backoff for failed operations

**Usage Example:**
```go
consumers := []consumerConfig{
    {config.UsersTopic, config.UsersNumPartitions, config.UsersConsumerGroupId, handleUserEvent},
    {config.OrdersTopic, config.OrdersNumPartitions, config.OrdersConsumerGroupId, handleOrderEvent},
}

for _, c := range consumers {
    go consumeEvents(&c)
}
```