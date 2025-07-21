package config

var KafkaBrokers = []string{"localhost:9092", "localhost:9093", "localhost:9094"}

const KafkaReplicationFactor int = 3

// topics and partitions
const (
	UsersTopic         string = "users"
	UsersNumPartitions int    = 3
)

const (
	OrdersTopic         string = "orders"
	OrdersNumPartitions int    = 5
)

const TestMsgKey string = "test"
