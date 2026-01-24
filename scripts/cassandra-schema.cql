CREATE KEYSPACE IF NOT EXISTS cdc_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};

USE cdc_keyspace;

-- users table
CREATE TABLE IF NOT EXISTS users (
    id uuid, 
    name text, 
    dob date, 
    created_at timestamp,
    modified_at timestamp,
    is_deleted boolean, 
    PRIMARY KEY (id, name)
);

-- orders table partitioned by order id
CREATE TABLE IF NOT EXISTS orders (
    order_id uuid,
    user_id uuid,
    status text, -- PLACED, SHIPPED, CANCELLED, DELIVERED
    quantity int,
    total_amount decimal,
    placed_at timestamp,
    modified_at timestamp,
    is_deleted boolean,
    PRIMARY KEY (order_id, user_id)
);

-- orders table partitioned by user_id
CREATE TABLE IF NOT EXISTS orders_by_user (
    user_id uuid,
    order_id uuid,
    status text, -- PLACED, SHIPPED, CANCELLED, DELIVERED
    quantity int,
    total_amount decimal,
    placed_at timestamp,
    modified_at timestamp,
    is_deleted boolean,
    PRIMARY KEY (user_id, order_id)
);

-- processed_events table for idempotency (insert with IF NOT EXISTS for deduplication)
-- `ts_ms` -> col to store event produce timestamp (timestamp when the connector produced the event) in ms
--            (cassandra doesn't support higher precision than ms for timestamps)
CREATE TABLE IF NOT EXISTS processed_events (
    event_id text PRIMARY KEY,
    topic text,
    ts_ms bigint,
    processed_at timestamp
);