package config

const KafkaBroker string = "localhost:9092"

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
