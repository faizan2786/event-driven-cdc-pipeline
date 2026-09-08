package sink

import (
	"testing"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/internal/model"
	"github.com/faizan2786/event-driven-cdc-pipeline/internal/sink/mocks"
	"github.com/golang/mock/gomock"
)

const testUserId string = "da0859fb-8eeb-44cd-97f5-df0db4f7a2c3"
const testOrderId string = "eed38f7e-fea3-46b4-9536-89a3b1cba1f8"

func TestAddUserEventToDB_create(t *testing.T) {

	// create a mock db service instance
	ctl := gomock.NewController(t)
	mdb := mocks.NewMockDBClient(ctl)

	dob := model.Date(time.Date(1990, 7, 2, 0, 0, 0, 0, time.UTC))
	currTime := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
	u := model.UserEvent{
		Type:      model.CREATE,
		UserId:    testUserId,
		Name:      "Test User",
		DOB:       &dob,
		CreatedAt: &currTime,
	}

	// assert a single Exec() call with expected query & params values
	dobStr := u.DOB.String()
	expectedParams := []any{u.UserId, u.Name, dobStr, u.CreatedAt}
	mdb.EXPECT().
		Exec("INSERT INTO users (id, name, dob, created_at) VALUES ($1, $2, $3, $4)", expectedParams...).
		Return(nil)

	// Call the function under test
	result := AddUserEventToDB(mdb, &u)
	if !result {
		t.Fatalf("expected AddUserEventToDB to return true, got false")
	}
}

func TestAddUserEventToDB_update(t *testing.T) {
	// create a mock db service instance
	ctl := gomock.NewController(t)
	mdb := mocks.NewMockDBClient(ctl)

	currTime := time.Date(2025, 6, 1, 10, 30, 0, 0, time.UTC)
	u := model.UserEvent{
		Type:       model.UPDATE,
		UserId:     testUserId,
		Name:       "Updated User",
		ModifiedAt: &currTime,
	}

	// assert a single Exec() call with expected query & params values
	expectedParams := []any{u.Name, u.ModifiedAt, u.UserId}
	mdb.EXPECT().
		Exec("UPDATE users SET name=$1, modified_at=$2 WHERE id=$3", expectedParams...).
		Return(nil)

	// Call the function under test
	result := AddUserEventToDB(mdb, &u)
	if !result {
		t.Fatalf("expected AddUserEventToDB to return true, got false")
	}
}

func TestAddUserEventToDB_delete(t *testing.T) {
	// create a mock db service instance
	ctl := gomock.NewController(t)
	mdb := mocks.NewMockDBClient(ctl)

	currTime := time.Date(2025, 6, 1, 10, 45, 0, 0, time.UTC)
	u := model.UserEvent{
		Type:       model.DELETE,
		UserId:     testUserId,
		ModifiedAt: &currTime,
	}

	// assert a single Exec() call with expected query & params values
	expectedParams := []any{u.ModifiedAt, u.UserId}
	mdb.EXPECT().
		Exec("UPDATE users SET is_deleted=true, modified_at=$1 WHERE id=$2", expectedParams...).
		Return(nil)

	// Call the function under test
	result := AddUserEventToDB(mdb, &u)
	if !result {
		t.Fatalf("expected AddUserEventToDB to return true, got false")
	}
}

func TestAddOrderEventToDB_create(t *testing.T) {
	// create a mock db service instance
	ctl := gomock.NewController(t)
	mdb := mocks.NewMockDBClient(ctl)

	placedAt := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
	o := model.OrderEvent{
		Type:       model.CREATE,
		OrderId:    testOrderId,
		Status:     model.PLACED,
		UserId:     testUserId,
		Quantity:   2,
		OrderTotal: 99.99,
		PlacedAt:   &placedAt,
	}

	// assert a single Exec() call with expected query & params values
	expectedParams := []any{o.OrderId, o.Status, o.UserId, o.Quantity, o.OrderTotal, o.PlacedAt}
	mdb.EXPECT().
		Exec("INSERT into orders (id, status, user_id, quantity, total_amount, placed_at) VALUES ($1, $2, $3, $4, $5, $6)", expectedParams...).
		Return(nil)

	// Call the function under test
	result := AddOrderEventToDB(mdb, &o)
	if !result {
		t.Fatalf("expected AddOrderEventToDB to return true, got false")
	}
}

func TestAddOrderEventToDB_update(t *testing.T) {
	// create a mock db service instance
	ctl := gomock.NewController(t)
	mdb := mocks.NewMockDBClient(ctl)

	modifiedAt := time.Date(2025, 6, 1, 11, 30, 0, 0, time.UTC)
	o := model.OrderEvent{
		Type:       model.UPDATE,
		OrderId:    testOrderId,
		Status:     model.SHIPPED,
		ModifiedAt: &modifiedAt,
	}

	// assert a single Exec() call with expected query & params values
	expectedParams := []any{o.Status, o.ModifiedAt, o.OrderId}
	mdb.EXPECT().
		Exec("UPDATE orders SET status=$1, modified_at=$2 WHERE id=$3", expectedParams...).
		Return(nil)

	// Call the function under test
	result := AddOrderEventToDB(mdb, &o)
	if !result {
		t.Fatalf("expected AddOrderEventToDB to return true, got false")
	}
}

func TestAddOrderEventToDB_delete(t *testing.T) {
	// create a mock db service instance
	ctl := gomock.NewController(t)
	mdb := mocks.NewMockDBClient(ctl)

	modifiedAt := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	o := model.OrderEvent{
		Type:       model.DELETE,
		OrderId:    testOrderId,
		Status:     model.CANCELLED,
		ModifiedAt: &modifiedAt,
	}

	// assert a single Exec() call with expected query & params values
	expectedParams := []any{o.Status, o.ModifiedAt, o.OrderId}
	mdb.EXPECT().
		Exec("UPDATE orders SET status=$1, modified_at=$2, is_deleted='T' WHERE id=$3", expectedParams...).
		Return(nil)

	// Call the function under test
	result := AddOrderEventToDB(mdb, &o)
	if !result {
		t.Fatalf("expected AddOrderEventToDB to return true, got false")
	}
}
