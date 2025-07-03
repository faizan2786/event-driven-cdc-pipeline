# cdc-pipeline Go Module


This directory contains the application code for the Event Driven CDC Pipeline project.
Currently, it provides commands to generate (produce) and consume `User` and `Order` events using Kafka and PostgreSQL.

## Prerequisites
- Docker and Docker Compose
- Go 1.23+ (recommended: Go 1.24+)

## Directory Structure

```
cdc-pipeline/
├── cmd/                        # CLI entrypoints
│   ├── produce/                # Produces random events to Kafka
│   |   └── event_client.go     # A test program to generate events and inspect in json format
│   └── consume/                # Consumes events from Kafka and writes to Postgres
├── internal/
│   ├── config/                 # Configuration for Kafka and Postgres
│   ├── consumer/               # Functions to write event data to Postgres
│   ├── model/                  # Event and data models
│   └── producer/               # Functions to generate random events
```

### Subdirectories
- **cmd/produce/**: CLI tool to generate and send random `User` and `Order` events to Kafka.
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