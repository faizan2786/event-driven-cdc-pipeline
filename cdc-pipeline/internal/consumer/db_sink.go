package consumer

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/faizan2786/system-design/cdc-pipeline/internal/config"
	"github.com/faizan2786/system-design/cdc-pipeline/internal/model"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func ConnectToDB() *sql.DB {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s", config.PGUser, config.PGPassword, config.PGAddr, config.PGDBName)

	var db *sql.DB
	var err error
	db, err = sql.Open("pgx", connStr)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	fmt.Printf("Successfully connected to PostgreSQL on %s\n", config.PGAddr)

	return db
}

// AddUserEventsToDB inserts user event data into the database.
//
// userData is the JSON payload of a user event read from Kafka.
func AddUserEventToDB(db *sql.DB, u model.UserEvent) bool {
	var err error

	switch u.Type {
	case model.CREATE:
		fmt.Println("Adding User")
		dob, err := u.DOB.MarshalJSON()
		if err != nil {
			fmt.Printf("AddUserEventsToDB: %v\n", err)
			return false
		}
		_, err = db.Exec("INSERT INTO users (id, name, dob, created_at) VALUES ($1, $2, $3, $4)", u.UserId, u.Name, string(dob), u.CreatedAt)
	case model.UPDATE:
		fmt.Println("Updating User")
		_, err = db.Exec("UPDATE users SET name=$1, modified_at=$2 WHERE id=$3", u.Name, u.ModifiedAt, u.UserId)
	case model.DELETE:
		fmt.Println("Deleting User")
		_, err = db.Exec("UPDATE users SET is_deleted=true, modified_at=$1 WHERE id=$2", u.ModifiedAt, u.UserId)
	default:
		err = fmt.Errorf("Unknown User event type.")
	}

	if err != nil {
		fmt.Printf("AddUserEventsToDB: %v\n", err)
		return false
	}
	return true
}
