# Event Driven CDC Pipeline

A real-time Change Data Capture (CDC) pipeline built with Go, demonstrating event-driven architecture using Kafka, PostgreSQL, and Cassandra. This project simulates producing business events, consuming them to update a primary database, and using CDC to keep external systems in sync.

The system uses `User` and `Order` events to demonstrate how to produce and consume business events, sink data to PostgreSQL, capture database changes via Debezium, and replicate to Cassandra as an external data sink.

## Architecture

- **3-Node Kafka Cluster** with KRaft mode (no Zookeeper required)
- **PostgreSQL** as primary database with logical replication
- **Debezium Kafka Connect** for Change Data Capture
- **Go producers/consumers** with idle timeout and graceful shutdown
- **Cassandra** as analytical database
- **Kafka UI** for monitoring and management

## Project Structure

```
event-driven-cdc-pipeline/
├── cdc-pipeline/                    # Go module containing all application code
│   ├── cmd/                        # CLI entrypoints
│   │   ├── produce/                # Event producer with topic management
│   │   └── consume/                # Multi-topic consumer with worker pools
│   ├── internal/                   # Internal Go packages
│   │   ├── config/                 # Configuration (multi-broker support)
│   │   ├── consumer/               # Database sink logic
│   │   ├── producer/               # Event generators
│   │   ├── model/                  # Event data structures
│   │   └── kafkautils/             # Kafka utilities (topic/group management)
│   └── go.mod, go.sum              # Go module files
├── docker-compose.yml              # Infrastructure services
├── table-creation.sql              # PostgreSQL schema initialization
└── configure-debezium.sh           # Debezium connector setup
```

- **cdc-pipeline/**: Main Go application with modular design
- **docker-compose.yml**: 3-node Kafka cluster + PostgreSQL + Cassandra + Debezium + Kafka UI

## Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| **Kafka Cluster** | | |
| kafka1 | 9092 | Broker 1 (external) |
| kafka2 | 9093 | Broker 2 (external) |
| kafka3 | 9094 | Broker 3 (external) |
| **Infrastructure** | | |
| PostgreSQL | 5432 | Primary database |
| Cassandra | 9042 | Analytics database |
| Debezium Connect | 8083 | CDC connector API |
| Kafka UI | 8080 | Web management interface |

## Key Features

- **High Availability**: 3-broker Kafka cluster with replication factor 3
- **Modular Design**: Easy to add new topics, consumers, and event handlers
- **Worker Pools**: One worker per partition for parallel processing
- **Failure Handling**: Retries with exponential backoff for failed operations
- **Topic Management**: Automatic topic creation with proper partitioning

## Quick Start

1. **Start the services:**
   ```bash
   docker-compose up -d
   ```

2. **Configure Debezium connector:**
   ```bash
   chmod +x configure-debezium.sh && ./configure-debezium.sh
   ```

3. **Build and run Go applications to produce/consume events:**
   ```bash
   cd cdc-pipeline
   
   # Install dependencies
   go mod tidy
   
   # Generate and produce events
   go run cmd/produce/main.go
   
   # Consume events with idle timeout
   go run cmd/consume/main.go
   ```

## Database Connection

Connect to PostgreSQL:
```bash
docker exec -it postgres psql -U postgres -d cdc_db
```

View table structure:
```sql
\d users    -- Describe users table
\d orders   -- Describe orders table
```

---

For more details, see the Go module's [README](cdc-pipeline/README.md).