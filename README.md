# Event Driven CDC Pipeline

A real-time Change Data Capture (CDC) pipeline built with Go, demonstrating event-driven architecture using Kafka, PostgreSQL, and Cassandra. This project simulates producing business events, consuming them to update a primary database, and using CDC to keep external systems in sync.

The system uses `User` and `Order` events to demonstrate how to produce and consume business events, sink data to PostgreSQL, capture database changes via Debezium, and sync the change to Cassandra as an external data sink.

## Architecture

- **3-Node Kafka Cluster** with KRaft mode (no Zookeeper required)
- **PostgreSQL** as primary database with logical replication
- **3-Node Cassandra Cluster** as an external (sink) database cluster with replication
- **Debezium Kafka Connect** for Change Data Capture from PostgreSQL to Kafka
- **Go producers/consumers** with idle timeout and graceful shutdown
- **CDC Consumer** for processing Debezium change events and syncing to Cassandra
- **Kafka UI** for monitoring and management

## Project Structure

```
event-driven-cdc-pipeline/
├── cdc-pipeline/                   # Go module containing the application code
├── docker-compose.yml              # Infrastructure services
├── postgres-schema.sql             # PostgreSQL schema initialization script
├── cassandra-schema.cql            # Cassandra schema initialization script
└── debezium-config.sh              # Debezium connector initialization
```
- **cdc-pipeline/**: Main Go application with modular design
- **docker-compose.yml**: Complete microservice infrastructure with 3-node Kafka + PostgreSQL + 3-node Cassandra + Debezium + Kafka UI

## Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| **Kafka Cluster** | | |
| kafka1 | 9092 | Broker 1 |
| kafka2 | 9093 | Broker 2 |
| kafka3 | 9094 | Broker 3 |
| **Infrastructure** | | |
| PostgreSQL | 5432 | Primary database |
| **Cassandra Cluster** | | |
| cassandra1 | 9042 | node 1 (Seed)|
| cassandra2 | 9043 | node 2 |
| cassandra3 | 9044 | node 3 |
| **CDC Management** | | |
| Debezium Connect | 8083 | CDC connector API |
| Kafka UI | 8080 | Web management interface |

## Key Features

- **High Availability**: 
  - 3-broker Kafka cluster with replication factor 3
  - 3-node Cassandra cluster with replication factor 3
- **Automatic Schema Setup**: 
  - PostgreSQL schema initialized on startup
  - Cassandra keyspace and tables created on startup
- **Modular Design**: Easy to add new topics, consumers, and event handlers
- **Worker Pools**: One worker per partition for parallel processing
- **Failure Handling**: Retries with exponential backoff for failed operations
- **Topic Management**: Automatic topic creation with proper partitioning
- **Custom Logging**: Three-level logger (INFO, DEBUG, ERROR) for basic monitoring and debugging
- **CDC Pipeline**: 
  - Debezium change event parser for PostgreSQL CDC events
  - Cassandra sink logic with idempotent operations
  - CDC consumer for processing change events with graceful shutdown

## Quick Start

**Start the services:**
   ```bash
   docker-compose up -d
   ```
Wait for all services to be up and running (it may take a few minutes).

**Check Cassandra cluster status:**
```bash
docker exec cassandra1 nodetool status
```
**Note:** Above command should show all 3 nodes listed with status `UN` (Up and Normal).
If any node is missing, check its logs for errors and **restart** the failed node using command such as:
```bash
docker-compose restart cassandra3
```

**Build and run Go applications to produce/consume events:**
   ```bash
   cd cdc-pipeline
   
   # Install dependencies
   go mod tidy
   
  # Generate and produce events
  go run ./cmd/producer

  # Consume events with idle timeout
  go run ./cmd/consumer
  
  # Consume CDC events from Debezium and sync to Cassandra
  go run ./cmd/cdcconsumer
   ```

## Database Connections

**Connect to PostgreSQL:**
```bash
docker exec -it postgres psql -U postgres -d cdc_db
```

View PostgreSQL table structure:
```sql
\d users    -- Describe users table
\d orders   -- Describe orders table
```

**Connect to Cassandra cluster:**
```bash
# Connect to any one Cassandra node
docker exec -it cassandra1 cqlsh
```

View table schema:
```cql
USE cdc_keyspace;
DESCRIBE TABLES;
DESCRIBE TABLE users;
DESCRIBE TABLE orders;
DESCRIBE TABLE orders_by_user;
```

---

For more details, see the Go module's [README](cdc-pipeline/README.md).