package sink

import (
	"database/sql"
	"fmt"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/config"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/logger"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type DBClient interface {
	Exec(query string, args ...any) error
	Close() error
}

// real implementation of the PGQuery interface
type realDBClient struct {
	db *sql.DB
}

func (q *realDBClient) Exec(query string, args ...any) error {
	_, err := q.db.Exec(query, args...)
	return err
}

func (q *realDBClient) Close() error {
	return q.db.Close()
}

// get a new real db client
func NewDBClient() (*realDBClient, error) {
	dbConn, err := connectToDB()
	if err != nil {
		return nil, fmt.Errorf("connectToDB: %w", err)
	}
	return &realDBClient{db: dbConn}, nil
}

func connectToDB() (*sql.DB, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s", config.PGUser, config.PGPassword, config.PGAddr, config.PGDBName)

	var db *sql.DB
	var err error
	db, err = sql.Open("pgx", connStr)
	if err != nil {
		return db, err
	}

	err = db.Ping()
	if err != nil {
		return db, err
	}
	logger.DebugLogger.Printf("Successfully connected to PostgreSQL on %s\n", config.PGAddr)

	return db, nil
}

// AddUserEventToDB inserts user event data into the database.
//
// userData is the JSON payload of a user event read from Kafka.
func AddUserEventToDB(dbClient DBClient, u *model.UserEvent) bool {
	var err error

	switch u.Type {
	case model.CREATE:
		logger.DebugLogger.Println("Adding User")
		dob := u.DOB.String()
		err = dbClient.Exec("INSERT INTO users (id, name, dob, created_at) VALUES ($1, $2, $3, $4)", u.UserId, u.Name, dob, u.CreatedAt)
	case model.UPDATE:
		logger.DebugLogger.Println("Updating User")
		err = dbClient.Exec("UPDATE users SET name=$1, modified_at=$2 WHERE id=$3", u.Name, u.ModifiedAt, u.UserId)
	case model.DELETE:
		logger.DebugLogger.Println("Deleting User")
		err = dbClient.Exec("UPDATE users SET is_deleted=true, modified_at=$1 WHERE id=$2", u.ModifiedAt, u.UserId)
	default:
		err = fmt.Errorf("unknown user event type")
	}

	if err != nil {
		logger.ErrorLogger.Printf("AddUserEventToDB: %v\n", err)
		return false
	}
	return true
}

// AddOrderEventToDB inserts order event data into the database.
//
// orderData is the JSON payload of a user event read from Kafka.
func AddOrderEventToDB(dbClient DBClient, o *model.OrderEvent) bool {

	var err error

	switch o.Type {
	case model.CREATE:
		logger.DebugLogger.Println("Creating an Order")
		err = dbClient.Exec("INSERT into orders (id, status, user_id, quantity, total_amount, placed_at) VALUES ($1, $2, $3, $4, $5, $6)", o.OrderId, o.Status, o.UserId, o.Quantity, o.OrderTotal, o.PlacedAt)
	case model.UPDATE:
		logger.DebugLogger.Println("Updating Order Status")
		err = dbClient.Exec("UPDATE orders SET status=$1, modified_at=$2 WHERE id=$3", o.Status, o.ModifiedAt, o.OrderId)
	case model.DELETE:
		logger.DebugLogger.Println("Cancelling an Order")
		err = dbClient.Exec("UPDATE orders SET status=$1, modified_at=$2, is_deleted='T' WHERE id=$3", o.Status, o.ModifiedAt, o.OrderId)
	default:
		err = fmt.Errorf("unknown order event type")
	}

	if err != nil {
		logger.ErrorLogger.Printf("AddOrderEventToDB: %v\n", err)
		return false
	}
	return true
}
