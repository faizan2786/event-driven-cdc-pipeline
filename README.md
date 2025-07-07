# Event Driven CDC Pipeline

This project implements a Change Data Capture (CDC) pipeline using Go, Kafka, PostgreSQL, and Cassandra. It uses `User` and `Order` events as examples to demonstrate how to produce and consume events to and from kafka, sink event data to a transactional DB, capture changes to data in the database via CDC (using Debezium) and then sink the captured change events to a secondary database (i.e. Cassandra), supporting event-driven architectures and analytics use cases.

## Project Structure


```
event-store-cdc-pipeline/
├── cdc-pipeline/         # Go module containing all application code
│   ├── cmd/              # CLI entrypoints for producing and consuming events
│   ├── internal/         # Internal Go packages (producer, consumer, config, model)
│   ├── go.mod, go.sum    # Go module files
├── docker-compose.yml    # Docker Compose for running dependencies (Kafka, Postgres, etc.)
├── table-creation.sql    # SQL script to initialize Postgres schema
├── configure-debezium.sh # Shell script to register Debezium connector for Postgres
```

- **cdc-pipeline/**: Main Go application (see its README for details)
- **docker-compose.yml**: Spins up Kafka, Postgres, Cassandra, Debezium, and Kafka UI
- **table-creation.sql**: Initializes the Postgres database schema

## Quickstart

1. **Start dependencies:**
   ```sh
   docker-compose up
   ```
   This will start all the required services: Kafka, Postgres (with schema), Cassandra, Debezium, and Kafka UI.

2. **Configure Debezium connector:**
   
   Run the provided shell script to register the Debezium PostgreSQL connector:
   ```sh
   chmod +x configure-debezium.sh   # Enable execution if not already
   ./configure-debezium.sh
   ```
   > **Note:** You must make the script executable before running it (see `chmod +x` above).

3. **Build and run Go commands:**
   See [cdc-pipeline/README.md](cdc-pipeline/README.md) for details on producing and consuming events.

---

For more details, see the Go module's [README](cdc-pipeline/README.md).