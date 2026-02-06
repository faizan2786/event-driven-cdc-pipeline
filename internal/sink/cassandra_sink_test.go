package sink

import (
	"testing"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/internal/model"
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

// alias for a list of query arguments
type queryArgs []any

// a struct to keep track of executed queries and their arguments
// (can be used for individual queries or a batch of multiple queries)
type executedQueries struct {
	queryStatements []string
	queryArguments  []queryArgs
}

// Implement CassandraQuery interface
type mockQuery struct{}

func (q *mockQuery) Exec() error                       { return nil }
func (q *mockQuery) MapScan(dest map[string]any) error { return nil }

// Implement CassandraBatch interface
type mockBatch struct {
	executedQueries
}

func (b *mockBatch) Query(stmt string, values ...any) {
	b.queryStatements = append(b.queryStatements, stmt)
	b.queryArguments = append(b.queryArguments, values)
}

// mockSession implements minimal gocql.Session interface for testing
type mockSession struct {
	queries executedQueries // list of executed individual queries in the sessions
	batches []*mockBatch    // list of executed batches in the session
}

// Implement CassandraSession interface
func (m *mockSession) Query(stmt string, values ...any) CassandraQuery {
	m.queries.queryStatements = append(m.queries.queryStatements, stmt)
	m.queries.queryArguments = append(m.queries.queryArguments, values)
	return &mockQuery{}
}

func (m *mockSession) NewBatch(beatType gocql.BatchType) CassandraBatch {
	return &mockBatch{}
}

func (m *mockSession) ExecuteBatch(batch CassandraBatch) error {
	m.batches = append(m.batches, batch.(*mockBatch))
	return nil
}

func TestApplyUserChange_Insert(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "c",
		Row: map[string]any{
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
	if len(mockSess.queries.queryStatements) != 1 {
		t.Fatalf("expected 1 executed query, got %d", len(mockSess.queries.queryStatements))
	}
	expectedQuery := "INSERT INTO users (id, name, dob, created_at, is_deleted) VALUES (?, ?, ?, ?, ?)"
	if mockSess.queries.queryStatements[0] != expectedQuery {
		t.Errorf("unexpected query executed: got %q, want %q", mockSess.queries.queryStatements[0], expectedQuery)
	}

	// check the query queryArguments
	createdAt, _ := time.Parse(time.RFC3339, ev.Row["created_at"].(string))
	expectedArgs := queryArgs{
		testUserUUID,
		"Alice",
		"2000-08-03",
		createdAt,
		false,
	}
	for i, arg := range expectedArgs {
		if mockSess.queries.queryArguments[0][i] != arg {
			t.Errorf("unexpected arg at position %d: got %v, want %v", i, mockSess.queries.queryArguments[0][i], arg)
		}
	}
}

// test update operation (i.e. name change)
func TestApplyUserChange_Update(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "u",
		Row: map[string]any{
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

	// the "UPDATE" should execute 1 non-batch query and 2 queries in a batch

	mockSess := client.session.(*mockSession)

	// check the non-batch query...

	if len(mockSess.queries.queryStatements) != 1 {
		t.Fatalf("expected 1 non-batch query, got %d", len(mockSess.queries.queryStatements))
	}
	expectedQuery := "SELECT name from users where id = ? LIMIT 1"
	if mockSess.queries.queryStatements[0] != expectedQuery {
		t.Errorf("unexpected query executed: got %q, want %q", mockSess.queries.queryStatements[0], expectedQuery)
	}

	// test query arguements
	expectedParams1 := queryArgs{
		testUserUUID,
	}
	qArgs := mockSess.queries.queryArguments[0]
	for i, val := range expectedParams1 {
		if qArgs[i] != val {
			t.Errorf("unexpected query argument at position %d, got %v, want %v", i, qArgs[i], val)
		}
	}

	// check the batched queries...

	if len(mockSess.batches) != 1 {
		t.Fatalf("expected 1 executed batch, got %d", len(mockSess.batches))
	}
	batch := mockSess.batches[0]
	if len(batch.queryStatements) != 2 {
		t.Fatalf("expected 2 queries in teh batch, got %d", len(batch.queryStatements))
	}

	queries := batch.queryStatements
	var expectedQueries = []string{
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

	// test INSERT query queryArguments
	expectedParams2 := queryArgs{
		testUserUUID,
		"Bob",
		"2000-08-03",
		createdAt,
		modifiedAt,
		false,
	}

	qArgs = batch.queryArguments[0]
	for i, val := range expectedParams2 {
		if qArgs[i] != val {
			t.Errorf("unexpected query argument at position %d, got %v, want %v", i, qArgs[i], val)
		}
	}
	// test DELETE query queryArguments
	expectedParams3 := queryArgs{
		testUserUUID,
		"",
	}

	qArgs = batch.queryArguments[1]
	for i, val := range expectedParams3 {
		if qArgs[i] != val {
			t.Errorf("unexpected query argument at position %d, got %v, want %v", i, qArgs[i], val)
		}
	}
}

// test update operation with is_delete = true
func TestApplyUserChange_Update_Delete(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "u",
		Row: map[string]any{
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
	if len(mockSess.queries.queryStatements) != 1 {
		t.Errorf("expected only 1 query to be executed, got %d", len(mockSess.queries.queryStatements))
	}

	query := mockSess.queries.queryStatements[0]
	expectedQuery := "UPDATE users SET modified_at = ?, is_deleted = ? WHERE id = ? and name = ?"

	if query != expectedQuery {
		t.Errorf("unexpected query: got %v, want %v", query, expectedQuery)
	}

	modifiedAt, _ := time.Parse(time.RFC3339, ev.Row["modified_at"].(string))
	expectedParams := queryArgs{
		modifiedAt,
		true,
		testUserUUID,
		"Bob",
	}

	qArgs := mockSess.queries.queryArguments[0]
	for i, val := range expectedParams {
		if qArgs[i] != val {
			t.Errorf("unexpected query argument at position %d, got %v, want %v", i, qArgs[i], val)
		}
	}

}

func TestApplyUserChange_InvalidOp(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "d",
		Row: map[string]any{
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
		Row: map[string]any{
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
	if len(mockSess.batches) != 1 {
		t.Fatalf("expected 1 executed batch, got %d", len(mockSess.batches))
	}
	batch := mockSess.batches[0]
	if len(batch.queryStatements) != 2 {
		t.Fatalf("expected 2 queries in batch, got %d", len(batch.queryStatements))
	}

	expectedQuery1 := "INSERT INTO orders (order_id, user_id, status, quantity, total_amount, placed_at, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?)"
	expectedQuery2 := "INSERT INTO orders_by_user (user_id, order_id, status, quantity, total_amount, placed_at, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?)"
	if batch.queryStatements[0] != expectedQuery1 {
		t.Errorf("unexpected query 1: got %q, want %q", batch.queryStatements[0], expectedQuery1)
	}
	if batch.queryStatements[1] != expectedQuery2 {
		t.Errorf("unexpected query 2: got %q, want %q", batch.queryStatements[1], expectedQuery2)
	}
	// check queryArguments for first query
	placedAt, _ := time.Parse(time.RFC3339, ev.Row["placed_at"].(string))
	expectedArgs1 := queryArgs{
		testOrderUUID,
		testUserUUID,
		"PLACED",
		2,
		inf.NewDec(10052, 2),
		placedAt,
		false,
	}
	for i, arg := range expectedArgs1 {
		if batch.queryArguments[0][i] != arg {
			// use comp method for quantity - a inf.Dec type
			if i == 4 && arg.(*inf.Dec).Cmp(batch.queryArguments[0][i].(*inf.Dec)) == 0 {
				continue
			}
			t.Errorf("unexpected arg for query 1 at position %d: got %v, want %v", i, batch.queryArguments[0][i], arg)
		}
	}
	// check queryArguments for second query
	expectedArgs2 := queryArgs{
		testUserUUID,
		testOrderUUID,
		"PLACED",
		2,
		inf.NewDec(10052, 2),
		placedAt,
		false,
	}
	for i, arg := range expectedArgs2 {
		if batch.queryArguments[1][i] != arg {
			// use comp method for quantity - a inf.Dec type
			if i == 4 && arg.(*inf.Dec).Cmp(batch.queryArguments[1][i].(*inf.Dec)) == 0 {
				continue
			}
			t.Errorf("unexpected arg for query 2 at position %d: got %v, want %v", i, batch.queryArguments[1][i], arg)
		}
	}
}

func TestApplyOrderChange_Update(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "u",
		Row: map[string]any{
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
	if len(mockSess.batches) != 1 {
		t.Fatalf("expected 1 executed batch, got %d", len(mockSess.batches))
	}
	batch := mockSess.batches[0]

	if len(batch.queryStatements) != 2 {
		t.Fatalf("expected 2 queries in batch, got %d", len(batch.queryStatements))
	}
	expectedQuery1 := "UPDATE orders SET status = ?, modified_at = ?, is_deleted = ? WHERE order_id = ? AND user_id = ?"
	expectedQuery2 := "UPDATE orders_by_user SET status = ?, modified_at = ?, is_deleted = ? WHERE user_id = ? AND order_id = ?"
	if batch.queryStatements[0] != expectedQuery1 {
		t.Errorf("unexpected query 1: got %q, want %q", batch.queryStatements[0], expectedQuery1)
	}
	if batch.queryStatements[1] != expectedQuery2 {
		t.Errorf("unexpected query 2: got %q, want %q", batch.queryStatements[1], expectedQuery2)
	}
	// check queryArguments for first query
	modifiedAt, _ := time.Parse(time.RFC3339, ev.Row["modified_at"].(string))
	expectedArgs1 := queryArgs{
		"CANCELLED",
		modifiedAt,
		true,
		testOrderUUID,
		testUserUUID,
	}
	for i, arg := range expectedArgs1 {
		if batch.queryArguments[0][i] != arg {
			t.Errorf("unexpected arg for query 1 at position %d: got %v, want %v", i, batch.queryArguments[0][i], arg)
		}
	}
	// check queryArguments for second query
	expectedArgs2 := queryArgs{
		"CANCELLED",
		modifiedAt,
		true,
		testUserUUID,
		testOrderUUID,
	}
	for i, arg := range expectedArgs2 {
		if batch.queryArguments[1][i] != arg {
			t.Errorf("unexpected arg for query 2 at position %d: got %v, want %v", i, batch.queryArguments[1][i], arg)
		}
	}
}

func TestApplyOrderChange_InvalidOp(t *testing.T) {
	client := &CassandraClient{session: &mockSession{}}

	ev := &model.ChangeEvent{
		Op: "d",
		Row: map[string]any{
			"id": "order-3",
		},
	}

	err := client.applyOrderChange(ev)
	if err != nil {
		t.Fatalf("expected no error for unsupported op, got %v", err)
	}
}
