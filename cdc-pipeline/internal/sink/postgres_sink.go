package sink

import (
	"database/sql"
	"fmt"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/config"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/logger"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func ConnectToDB() (*sql.DB, error) {
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
func AddUserEventToDB(db *sql.DB, u model.UserEvent) bool {
	var err error

	switch u.Type {
	case model.CREATE:
		logger.DebugLogger.Println("Adding User")
		dob := u.DOB.String()
		_, err = db.Exec("INSERT INTO users (id, name, dob, created_at) VALUES ($1, $2, $3, $4)", u.UserId, u.Name, dob, u.CreatedAt)
	case model.UPDATE:
		logger.DebugLogger.Println("Updating User")
		_, err = db.Exec("UPDATE users SET name=$1, modified_at=$2 WHERE id=$3", u.Name, u.ModifiedAt, u.UserId)
	case model.DELETE:
		logger.DebugLogger.Println("Deleting User")
		_, err = db.Exec("UPDATE users SET is_deleted=true, modified_at=$1 WHERE id=$2", u.ModifiedAt, u.UserId)
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
func AddOrderEventToDB(db *sql.DB, o model.OrderEvent) bool {

	var err error

	switch o.Type {
	case model.CREATE:
		logger.DebugLogger.Println("Creating an Order")
		_, err = db.Exec("INSERT into orders (id, status, user_id, quantity, total_amount, placed_at) VALUES ($1, $2, $3, $4, $5, $6)", o.OrderId, o.Status, o.UserId, o.Quantity, o.OrderTotal, o.PlacedAt)
	case model.UPDATE:
		logger.DebugLogger.Println("Updating Order Status")
		_, err = db.Exec("UPDATE orders SET status=$1, modified_at=$2 WHERE id=$3", o.Status, o.ModifiedAt, o.OrderId)
	case model.DELETE:
		logger.DebugLogger.Println("Cancelling an Order")
		_, err = db.Exec("UPDATE orders SET status=$1, modified_at=$2, is_deleted='T' WHERE id=$3", o.Status, o.ModifiedAt, o.OrderId)
	default:
		err = fmt.Errorf("unknown order event type")
	}

	if err != nil {
		logger.ErrorLogger.Printf("AddOrderEventToDB: %v\n", err)
		return false
	}
	return true
}
