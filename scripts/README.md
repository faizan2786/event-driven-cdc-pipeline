# Scripts

This directory contains setup and configuration scripts for the project.

## Files

- **`debezium-config.sh`** - Configuration script for setting up Debezium PostgreSQL connector

## Usage

The scripts are automatically executed by Docker Compose during the infrastructure setup. Manual execution is typically not required, but can be done for troubleshooting:

```bash
# Make sure Kafka Connect is running first
docker exec debezium-kafka-connect bash /debezium-config.sh
```
