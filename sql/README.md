# Database Schemas

This directory contains SQL (and related) files for initializing database schemas.

## Files

- **`postgres-schema.sql`** - PostgreSQL database schema
- **`cassandra-schema.cql`** - Cassandra database schema  

## Usage

These schema files are automatically executed during Docker Compose startup:

- **PostgreSQL**: Mounted to `/docker-entrypoint-initdb.d/` for automatic initialization
- **Cassandra**: Executed by the `cassandra-init` service after cluster startup