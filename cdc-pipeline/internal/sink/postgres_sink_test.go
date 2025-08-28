package sink

import (
	"testing"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
)

const testUserId string = "da0859fb-8eeb-44cd-97f5-df0db4f7a2c3"
const testOrderId string = "eed38f7e-fea3-46b4-9536-89a3b1cba1f8"

func TestConnToDB(t *testing.T) {
	_, err := ConnectToDB()
	if err != nil {
		t.Errorf("DB connection was not successful.\n%v", err)
	}
}

func TestInsertUser(t *testing.T) {
	db, err := ConnectToDB()
	if err != nil {
		t.Errorf("Failed to connect to the DB:\n%v", err)
	}
	defer db.Close()

	dob := model.Date(time.Date(1990, 7, 2, 0, 0, 0, 0, time.UTC))
	currTime := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
	u := model.UserEvent{
		UserId:    testUserId,
		Name:      "Test User",
		DOB:       &dob,
		CreatedAt: &currTime,
	}

	t.Log("Inserting a User")
	// Convert DOB to string in YYYY-MM-DD format
	dobStr := u.DOB.String()
	res, err := db.Exec("INSERT INTO users (id, name, dob, created_at) VALUES ($1, $2, $3, $4)", u.UserId, u.Name, dobStr, u.CreatedAt)

	if err != nil {
		t.Errorf("Error while inserting user into DB:\n%v", err)
	} else {
		numRows, _ := res.RowsAffected()
		t.Logf("# Rows inserted: %d\n", numRows)
	}
}

func TestInsertOrder(t *testing.T) {
	db, err := ConnectToDB()
	if err != nil {
		t.Errorf("Failed to connect to the DB:\n%v", err)
	}
	defer db.Close()

	o := model.OrderEvent{
		OrderId:    testOrderId,
		Status:     "FAKE",
		UserId:     testUserId,
		OrderTotal: 100.52,
	}
	t.Log("Creating an Order")
	res, err := db.Exec("INSERT into orders (id, status, user_id, quantity, total_amount) VALUES ($1, $2, $3, $4, $5)", o.OrderId, o.Status, o.UserId, o.Quantity, o.OrderTotal)

	if err != nil {
		t.Errorf("Error while inserting order into DB:\n%v", err)
	} else {
		numRows, _ := res.RowsAffected()
		t.Logf("# Rows inserted: %d\n", numRows)
	}
}

func TestDeleteOrder(t *testing.T) {
	db, err := ConnectToDB()

	if err != nil {
		t.Errorf("Failed to connect to the DB:\n%v", err)
	}
	defer db.Close()

	t.Log("Deleting the Order record")
	res, err := db.Exec("DELETE from orders WHERE id=$1", testOrderId)

	if err != nil {
		t.Errorf("Error while deleting order from DB:\n%v", err)
	} else {
		numRows, _ := res.RowsAffected()
		t.Logf("# Rows deleted: %d\n", numRows)
	}
}

func TestDeleteUser(t *testing.T) {
	db, err := ConnectToDB()
	if err != nil {
		t.Errorf("Failed to connect to the DB:\n%v", err)
	}
	defer db.Close()

	t.Log("Deleting the User record")
	res, err := db.Exec("DELETE from users WHERE id=$1", testUserId)

	if err != nil {
		t.Errorf("Error while deleting user from DB:\n%v", err)
	} else {
		numRows, _ := res.RowsAffected()
		t.Logf("# Rows deleted: %d\n", numRows)
	}
}
