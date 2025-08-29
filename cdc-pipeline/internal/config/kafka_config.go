package config

var KafkaBrokers = []string{"localhost:9092", "localhost:9093", "localhost:9094"}

const KafkaReplicationFactor int = 3

// topics and partitions
const (
	UsersTopic           string = "users"
	UsersNumPartitions   int    = 3
	UsersConsumerGroupId string = "go-consumer-group-users"
)

const (
	OrdersTopic           string = "orders"
	OrdersNumPartitions   int    = 5
	OrdersConsumerGroupId string = "go-consumer-group-orders"
)

const TestMsgKey string = "test"

// debezium topics
const (
	DebeziumUsersTopic  string = "cdc.public.users"
	DebeziumOrdersTopic string = "cdc.public.orders"
)
