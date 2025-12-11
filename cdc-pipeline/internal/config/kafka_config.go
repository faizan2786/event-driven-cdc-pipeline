package config

var KafkaBrokers = []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"}

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
