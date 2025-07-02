package consumer

import (
	"testing"

	"github.com/faizan2786/system-design/cdc-pipeline/internal/model"
)

func TestConnToDB(t *testing.T) {
	_, err := ConnectToDB()
	if err != nil {
		t.Errorf("DB connection was not successful.\n%v", err)
	}
}

func TestInsertOrder(t *testing.T) {
	db, err := ConnectToDB()
	if err != nil {
		t.Errorf("Failed to connect to the DB:\n%v", err)
	}
	defer db.Close()

	o := model.OrderEvent{
		OrderId:    "eed38f7e-fea3-46b4-9536-89a3b1cba1f8",
		Status:     "FAKE",
		UserId:     "da0859fb-8eeb-44cd-97f5-df0db4f7a2c3",
		Quantity:   10,
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

	orderId := "eed38f7e-fea3-46b4-9536-89a3b1cba1f8"
	res, err := db.Exec("DELETE from orders WHERE id=$1", orderId)

	if err != nil {
		t.Errorf("Error while deleting order from DB:\n%v", err)
	} else {
		numRows, _ := res.RowsAffected()
		t.Logf("# Rows deleted: %d\n", numRows)
	}
}
