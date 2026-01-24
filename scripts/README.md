# Scripts and Schemas

This directory provides setup and initial configuration scripts for the containers used in this project.

## Files

- **`debezium-config.sh`** - Configuration script for setting up PostgreSQL connector in Debezium
- **`postgres-schema.sql`** - PostgreSQL database schema
- **`cassandra-schema.cql`** - Cassandra database schema

## Usage

### Scripts
The scripts are automatically executed by Docker Compose during the infrastructure setup. Manual execution is typically not required, but can be done for troubleshooting:

```bash
# Make sure Kafka Connect is running first
docker exec debezium-kafka-connect bash /debezium-config.sh
```

### Schemas
These database schema files are automatically executed during Docker Compose startup:

- **PostgreSQL**: Mounted to `/docker-entrypoint-initdb.d/` for automatic initialization
- **Cassandra**: Executed by the `cassandra-init` service after cluster startup