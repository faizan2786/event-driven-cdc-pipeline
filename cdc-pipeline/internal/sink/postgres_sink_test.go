package sink

import (
	"testing"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
)

// define mock db client
type mockDBClient struct {
	executedQueries []string
	queryParams     [][]any // slice of parameters for each executed query
}

func (db *mockDBClient) Exec(query string, params ...any) error {
	db.executedQueries = append(db.executedQueries, query)
	db.queryParams = append(db.queryParams, params)
	return nil
}

func (db *mockDBClient) Close() error {
	return nil // nothing to do
}

const testUserId string = "da0859fb-8eeb-44cd-97f5-df0db4f7a2c3"
const testOrderId string = "eed38f7e-fea3-46b4-9536-89a3b1cba1f8"

func TestAddUserEventToDB_create(t *testing.T) {
	db := &mockDBClient{}

	dob := model.Date(time.Date(1990, 7, 2, 0, 0, 0, 0, time.UTC))
	currTime := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
	u := model.UserEvent{
		Type:      model.CREATE,
		UserId:    testUserId,
		Name:      "Test User",
		DOB:       &dob,
		CreatedAt: &currTime,
	}

	// Call the function under test
	result := AddUserEventToDB(db, &u)
	if !result {
		t.Fatalf("expected AddUserEventToDB to return true, got false")
	}

	// Verify the query was executed
	if len(db.executedQueries) != 1 {
		t.Fatalf("expected 1 executed query, got %d", len(db.executedQueries))
	}

	expectedQuery := "INSERT INTO users (id, name, dob, created_at) VALUES ($1, $2, $3, $4)"
	if db.executedQueries[0] != expectedQuery {
		t.Errorf("unexpected query executed: got %q, want %q", db.executedQueries[0], expectedQuery)
	}

	// Verify the query parameters
	if len(db.queryParams) != 1 {
		t.Fatalf("expected 1 set of query params, got %d", len(db.queryParams))
	}

	dobStr := u.DOB.String()
	expectedParams := []any{u.UserId, u.Name, dobStr, u.CreatedAt}
	params := db.queryParams[0]

	if len(params) != len(expectedParams) {
		t.Fatalf("expected %d params, got %d", len(expectedParams), len(params))
	}

	for i, expected := range expectedParams {
		if params[i] != expected {
			t.Errorf("unexpected param at position %d: got %v, want %v", i, params[i], expected)
		}
	}
}

func TestAddUserEventToDB_update(t *testing.T) {
	db := &mockDBClient{}

	currTime := time.Date(2025, 6, 1, 10, 30, 0, 0, time.UTC)
	u := model.UserEvent{
		Type:       model.UPDATE,
		UserId:     testUserId,
		Name:       "Updated User",
		ModifiedAt: &currTime,
	}

	// Call the function under test
	result := AddUserEventToDB(db, &u)
	if !result {
		t.Fatalf("expected AddUserEventToDB to return true, got false")
	}

	// Verify the query was executed
	if len(db.executedQueries) != 1 {
		t.Fatalf("expected 1 executed query, got %d", len(db.executedQueries))
	}

	expectedQuery := "UPDATE users SET name=$1, modified_at=$2 WHERE id=$3"
	if db.executedQueries[0] != expectedQuery {
		t.Errorf("unexpected query executed: got %q, want %q", db.executedQueries[0], expectedQuery)
	}

	// Verify the query parameters
	expectedParams := []any{u.Name, u.ModifiedAt, u.UserId}
	params := db.queryParams[0]

	if len(params) != len(expectedParams) {
		t.Fatalf("expected %d params, got %d", len(expectedParams), len(params))
	}

	for i, expected := range expectedParams {
		if params[i] != expected {
			t.Errorf("unexpected param at position %d: got %v, want %v", i, params[i], expected)
		}
	}
}

func TestAddUserEventToDB_delete(t *testing.T) {
	db := &mockDBClient{}

	currTime := time.Date(2025, 6, 1, 10, 45, 0, 0, time.UTC)
	u := model.UserEvent{
		Type:       model.DELETE,
		UserId:     testUserId,
		ModifiedAt: &currTime,
	}

	// Call the function under test
	result := AddUserEventToDB(db, &u)
	if !result {
		t.Fatalf("expected AddUserEventToDB to return true, got false")
	}

	// Verify the query was executed
	if len(db.executedQueries) != 1 {
		t.Fatalf("expected 1 executed query, got %d", len(db.executedQueries))
	}

	expectedQuery := "UPDATE users SET is_deleted=true, modified_at=$1 WHERE id=$2"
	if db.executedQueries[0] != expectedQuery {
		t.Errorf("unexpected query executed: got %q, want %q", db.executedQueries[0], expectedQuery)
	}

	// Verify the query parameters
	expectedParams := []any{u.ModifiedAt, u.UserId}
	params := db.queryParams[0]

	if len(params) != len(expectedParams) {
		t.Fatalf("expected %d params, got %d", len(expectedParams), len(params))
	}

	for i, expected := range expectedParams {
		if params[i] != expected {
			t.Errorf("unexpected param at position %d: got %v, want %v", i, params[i], expected)
		}
	}
}

func TestAddOrderEventToDB_create(t *testing.T) {
	db := &mockDBClient{}

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

	// Call the function under test
	result := AddOrderEventToDB(db, &o)
	if !result {
		t.Fatalf("expected AddOrderEventToDB to return true, got false")
	}

	// Verify the query was executed
	if len(db.executedQueries) != 1 {
		t.Fatalf("expected 1 executed query, got %d", len(db.executedQueries))
	}

	expectedQuery := "INSERT into orders (id, status, user_id, quantity, total_amount, placed_at) VALUES ($1, $2, $3, $4, $5, $6)"
	if db.executedQueries[0] != expectedQuery {
		t.Errorf("unexpected query executed: got %q, want %q", db.executedQueries[0], expectedQuery)
	}

	// Verify the query parameters
	expectedParams := []any{o.OrderId, o.Status, o.UserId, o.Quantity, o.OrderTotal, o.PlacedAt}
	params := db.queryParams[0]

	if len(params) != len(expectedParams) {
		t.Fatalf("expected %d params, got %d", len(expectedParams), len(params))
	}

	for i, expected := range expectedParams {
		if params[i] != expected {
			t.Errorf("unexpected param at position %d: got %v, want %v", i, params[i], expected)
		}
	}
}

func TestAddOrderEventToDB_update(t *testing.T) {
	db := &mockDBClient{}

	modifiedAt := time.Date(2025, 6, 1, 11, 30, 0, 0, time.UTC)
	o := model.OrderEvent{
		Type:       model.UPDATE,
		OrderId:    testOrderId,
		Status:     model.SHIPPED,
		ModifiedAt: &modifiedAt,
	}

	// Call the function under test
	result := AddOrderEventToDB(db, &o)
	if !result {
		t.Fatalf("expected AddOrderEventToDB to return true, got false")
	}

	// Verify the query was executed
	if len(db.executedQueries) != 1 {
		t.Fatalf("expected 1 executed query, got %d", len(db.executedQueries))
	}

	expectedQuery := "UPDATE orders SET status=$1, modified_at=$2 WHERE id=$3"
	if db.executedQueries[0] != expectedQuery {
		t.Errorf("unexpected query executed: got %q, want %q", db.executedQueries[0], expectedQuery)
	}

	// Verify the query parameters
	expectedParams := []any{o.Status, o.ModifiedAt, o.OrderId}
	params := db.queryParams[0]

	if len(params) != len(expectedParams) {
		t.Fatalf("expected %d params, got %d", len(expectedParams), len(params))
	}

	for i, expected := range expectedParams {
		if params[i] != expected {
			t.Errorf("unexpected param at position %d: got %v, want %v", i, params[i], expected)
		}
	}
}

func TestAddOrderEventToDB_delete(t *testing.T) {
	db := &mockDBClient{}

	modifiedAt := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	o := model.OrderEvent{
		Type:       model.DELETE,
		OrderId:    testOrderId,
		Status:     model.CANCELLED,
		ModifiedAt: &modifiedAt,
	}

	// Call the function under test
	result := AddOrderEventToDB(db, &o)
	if !result {
		t.Fatalf("expected AddOrderEventToDB to return true, got false")
	}

	// Verify the query was executed
	if len(db.executedQueries) != 1 {
		t.Fatalf("expected 1 executed query, got %d", len(db.executedQueries))
	}

	expectedQuery := "UPDATE orders SET status=$1, modified_at=$2, is_deleted='T' WHERE id=$3"
	if db.executedQueries[0] != expectedQuery {
		t.Errorf("unexpected query executed: got %q, want %q", db.executedQueries[0], expectedQuery)
	}

	// Verify the query parameters
	expectedParams := []any{o.Status, o.ModifiedAt, o.OrderId}
	params := db.queryParams[0]

	if len(params) != len(expectedParams) {
		t.Fatalf("expected %d params, got %d", len(expectedParams), len(params))
	}

	for i, expected := range expectedParams {
		if params[i] != expected {
			t.Errorf("unexpected param at position %d: got %v, want %v", i, params[i], expected)
		}
	}
}
