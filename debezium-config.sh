#!/bin/bash

curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "cdc-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "cdc_db",
      "topic.prefix": "cdc",
      "plugin.name": "pgoutput",
      "table.include.list": "public.users,public.orders",
      "slot.name": "cdc_slot",
      "publication.name": "cdc_pub",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter"
    }
  }' \
  http://debezium:8083/connectors