package sink

import (
	"testing"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/config"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

const testUserID string = "da0859fb-8eeb-44cd-97f5-df0db4f7a2c3"
const testOrderID string = "eed38f7e-fea3-46b4-9536-89a3b1cba1f8"

var testUserUUID gocql.UUID = parseUUID(testUserID)
var testOrderUUID gocql.UUID = parseUUID(testOrderID)

func parseUUID(id string) gocql.UUID {
	uuid, err := gocql.ParseUUID(id)
	if err != nil {
		panic(err)
	}
	return uuid
}

func TestConnToCassandra(t *testing.T) {
	client, err := NewCassandraClient(config.CassandraHosts, config.CassandraKeyspace)
	if err != nil {
		t.Errorf("Error while creating a new Cassandra session.\n%v\n", err)
	} else {
		client.Close()
	}
}

// mockSession implements minimal gocql.Session interface for testing
type mockSession struct {
	executedQueries []string
	args            [][]interface{}
}

// Implement CassandraSession interface
func (m *mockSession) Query(stmt string, values ...interface{}) CassandraQuery {
	m.executedQueries = append(m.executedQueries, stmt)
	m.args = append(m.args, values)
	return &mockQuery{}
}

type mockQuery struct{}

// Implement CassandraQuery interface
func (q *mockQuery) Exec() error                               { return nil }
func (q *mockQuery) MapScan(dest map[string]interface{}) error { return nil }

func TestApplyUserChange_Insert(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "c",
		Row: map[string]interface{}{
			"id":          testUserID,
			"name":        "Alice",
			"dob":         11172,
			"is_deleted":  false,
			"created_at":  "2025-08-28T16:02:58.281604Z",
			"modified_at": "2025-08-28T16:02:58.281607Z",
		},
	}

	err := client.applyUserChange(ev)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	mockSess := client.session.(*mockSession)

	// check the query
	if len(mockSess.executedQueries) != 1 {
		t.Fatalf("expected 1 executed query, got %d", len(mockSess.executedQueries))
	}
	expectedQuery := "INSERT INTO users (id, name, dob, created_at, is_deleted) VALUES (?, ?, ?, ?, ?)"
	if mockSess.executedQueries[0] != expectedQuery {
		t.Errorf("unexpected query executed: got %q, want %q", mockSess.executedQueries[0], expectedQuery)
	}

	// check the query args
	createdAt, _ := time.Parse(time.RFC3339, ev.Row["created_at"].(string))
	expectedArgs := []interface{}{
		testUserUUID,
		"Alice",
		"2000-08-03",
		createdAt,
		false,
	}
	for i, arg := range expectedArgs {
		if mockSess.args[0][i] != arg {
			t.Errorf("unexpected arg at position %d: got %v, want %v", i, mockSess.args[0][i], arg)
		}
	}
}

// test update operation (i.e. name change)
func TestApplyUserChange_Update(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "u",
		Row: map[string]interface{}{
			"id":          testUserID,
			"name":        "Bob",
			"dob":         11172,
			"is_deleted":  false,
			"created_at":  "2025-08-28T16:02:58.281604Z",
			"modified_at": "2025-08-28T16:02:58.281607Z",
		},
	}

	err := client.applyUserChange(ev)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	mockSess := client.session.(*mockSession)
	if len(mockSess.executedQueries) != 3 {
		t.Fatalf("expected 3 queries to be executed, got %d", len(mockSess.executedQueries))
	}

	queries := mockSess.executedQueries
	var expectedQueries = []string{
		"SELECT name from users where id = ? LIMIT 1",
		"INSERT INTO users (id, name, dob, created_at, modified_at, is_deleted) VALUES (?, ?, ?, ?, ?, ?)",
		"DELETE FROM users WHERE id = ? and name = ?",
	}

	for i, query := range queries {
		if query != expectedQueries[i] {
			t.Errorf("unexpected query: got %v, want %v", query, expectedQueries[i])
		}
	}

	createdAt, _ := time.Parse(time.RFC3339, ev.Row["created_at"].(string))
	modifiedAt, _ := time.Parse(time.RFC3339, ev.Row["modified_at"].(string))

	// test SELECT query args
	expectedParams1 := []interface{}{
		testUserUUID,
	}

	queryArgs := mockSess.args[0]
	for i, val := range expectedParams1 {
		if queryArgs[i] != val {
			t.Errorf("unexpected query argument at position %d, got %v, want %v", i, queryArgs[i], val)
		}
	}

	// test INSERT query args
	expectedParams2 := []interface{}{
		testUserUUID,
		"Bob",
		"2000-08-03",
		createdAt,
		modifiedAt,
		false,
	}

	queryArgs = mockSess.args[1]
	for i, val := range expectedParams2 {
		if queryArgs[i] != val {
			t.Errorf("unexpected query argument at position %d, got %v, want %v", i, queryArgs[i], val)
		}
	}
	// test DELETE query args
	expectedParams3 := []interface{}{
		testUserUUID,
		"",
	}

	queryArgs = mockSess.args[2]
	for i, val := range expectedParams3 {
		if queryArgs[i] != val {
			t.Errorf("unexpected query argument at position %d, got %v, want %v", i, queryArgs[i], val)
		}
	}
}

// test update operation with is_delete = true
func TestApplyUserChange_Update_Delete(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "u",
		Row: map[string]interface{}{
			"id":          testUserID,
			"name":        "Bob",
			"dob":         11172,
			"is_deleted":  true,
			"created_at":  "2025-08-28T16:02:58.281604Z",
			"modified_at": "2025-08-28T16:02:58.281607Z",
		},
	}

	err := client.applyUserChange(ev)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	mockSess := client.session.(*mockSession)
	if len(mockSess.executedQueries) != 1 {
		t.Errorf("expected only 1 query to be executed, got %d", len(mockSess.executedQueries))
	}

	query := mockSess.executedQueries[0]
	expectedQuery := "UPDATE users SET modified_at = ?, is_deleted = ? WHERE id = ? and name = ?"

	if query != expectedQuery {
		t.Errorf("unexpected query: got %v, want %v", query, expectedQuery)
	}

	modifiedAt, _ := time.Parse(time.RFC3339, ev.Row["modified_at"].(string))
	expectedParams := []interface{}{
		modifiedAt,
		true,
		testUserUUID,
		"Bob",
	}

	queryArgs := mockSess.args[0]
	for i, val := range expectedParams {
		if queryArgs[i] != val {
			t.Errorf("unexpected query argument at position %d, got %v, want %v", i, queryArgs[i], val)
		}
	}

}

func TestApplyUserChange_InvalidOp(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "d",
		Row: map[string]interface{}{
			"id": "user-3",
		},
	}

	err := client.applyUserChange(ev)
	if err != nil {
		t.Fatalf("expected no error for unsupported op, got %v", err)
	}
}

func TestApplyOrderChange_Insert(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "c",
		Row: map[string]interface{}{
			"id":           testOrderID,
			"user_id":      testUserID,
			"status":       "PLACED",
			"quantity":     2,
			"total_amount": "J0Q=", // base64 for 10052 (100.52 with scale 2)
			"placed_at":    "2025-08-28T16:02:58.281604Z",
			"is_deleted":   false,
		},
	}

	err := client.applyOrderChange(ev)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	mockSess := client.session.(*mockSession)
	if len(mockSess.executedQueries) != 2 {
		t.Fatalf("expected 2 executed queries, got %d", len(mockSess.executedQueries))
	}
	expectedQuery1 := "INSERT INTO orders (order_id, user_id, status, quantity, total_amount, placed_at, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?)"
	expectedQuery2 := "INSERT INTO orders_by_user (user_id, order_id, status, quantity, total_amount, placed_at, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?)"
	if mockSess.executedQueries[0] != expectedQuery1 {
		t.Errorf("unexpected query 1: got %q, want %q", mockSess.executedQueries[0], expectedQuery1)
	}
	if mockSess.executedQueries[1] != expectedQuery2 {
		t.Errorf("unexpected query 2: got %q, want %q", mockSess.executedQueries[1], expectedQuery2)
	}
	// check args for first query
	placedAt, _ := time.Parse(time.RFC3339, ev.Row["placed_at"].(string))
	expectedArgs1 := []interface{}{
		testOrderUUID,
		testUserUUID,
		"PLACED",
		2,
		inf.NewDec(10052, 2),
		placedAt,
		false,
	}
	for i, arg := range expectedArgs1 {
		if mockSess.args[0][i] != arg {
			// use comp method for quantity - a inf.Dec type
			if i == 4 && arg.(*inf.Dec).Cmp(mockSess.args[0][i].(*inf.Dec)) == 0 {
				continue
			}
			t.Errorf("unexpected arg for query 1 at position %d: got %v, want %v", i, mockSess.args[0][i], arg)
		}
	}
	// check args for second query
	expectedArgs2 := []interface{}{
		testUserUUID,
		testOrderUUID,
		"PLACED",
		2,
		inf.NewDec(10052, 2),
		placedAt,
		false,
	}
	for i, arg := range expectedArgs2 {
		if mockSess.args[1][i] != arg {
			// use comp method for quantity - a inf.Dec type
			if i == 4 && arg.(*inf.Dec).Cmp(mockSess.args[1][i].(*inf.Dec)) == 0 {
				continue
			}
			t.Errorf("unexpected arg for query 2 at position %d: got %v, want %v", i, mockSess.args[1][i], arg)
		}
	}
}

func TestApplyOrderChange_Update(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "u",
		Row: map[string]interface{}{
			"id":           testOrderID,
			"user_id":      testUserID,
			"status":       "CANCELLED",
			"quantity":     1,
			"total_amount": "J0Q=", // base64 for 10052 (100.52 with scale 2)
			"placed_at":    "2025-08-28T16:02:58.281604Z",
			"modified_at":  "2025-08-28T16:52:58.281604Z",
			"is_deleted":   true,
		},
	}

	err := client.applyOrderChange(ev)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	mockSess := client.session.(*mockSession)
	if len(mockSess.executedQueries) != 2 {
		t.Fatalf("expected 2 executed queries, got %d", len(mockSess.executedQueries))
	}
	expectedQuery1 := "UPDATE orders SET status = ?, modified_at = ?, is_deleted = ? WHERE order_id = ? AND user_id = ?"
	expectedQuery2 := "UPDATE orders_by_user SET status = ?, modified_at = ?, is_deleted = ? WHERE user_id = ? AND order_id = ?"
	if mockSess.executedQueries[0] != expectedQuery1 {
		t.Errorf("unexpected query 1: got %q, want %q", mockSess.executedQueries[0], expectedQuery1)
	}
	if mockSess.executedQueries[1] != expectedQuery2 {
		t.Errorf("unexpected query 2: got %q, want %q", mockSess.executedQueries[1], expectedQuery2)
	}
	// check args for first query
	modifiedAt, _ := time.Parse(time.RFC3339, ev.Row["modified_at"].(string))
	expectedArgs1 := []interface{}{
		"CANCELLED",
		modifiedAt,
		true,
		testOrderUUID,
		testUserUUID,
	}
	for i, arg := range expectedArgs1 {
		if mockSess.args[0][i] != arg {
			t.Errorf("unexpected arg for query 1 at position %d: got %v, want %v", i, mockSess.args[0][i], arg)
		}
	}
	// check args for second query
	expectedArgs2 := []interface{}{
		"CANCELLED",
		modifiedAt,
		true,
		testUserUUID,
		testOrderUUID,
	}
	for i, arg := range expectedArgs2 {
		if mockSess.args[1][i] != arg {
			t.Errorf("unexpected arg for query 2 at position %d: got %v, want %v", i, mockSess.args[1][i], arg)
		}
	}
}

func TestApplyOrderChange_InvalidOp(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "d",
		Row: map[string]interface{}{
			"id": "order-3",
		},
	}

	err := client.applyOrderChange(ev)
	if err != nil {
		t.Fatalf("expected no error for unsupported op, got %v", err)
	}
}
