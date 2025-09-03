package sink

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/config"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/logger"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/parser"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

// define a Session and Query interface (for testability)
type CassandraSession interface {
	Query(stmt string, values ...interface{}) CassandraQuery
}

type CassandraQuery interface {
	Exec() error
	MapScan(map[string]interface{}) error
}

// CassandraClient wraps a CassandraSession (interface)
type CassandraClient struct {
	session CassandraSession
}

// Adapter for real gocql.Session
type realSession struct {
	sess *gocql.Session
}

func (r *realSession) Query(stmt string, values ...interface{}) CassandraQuery {
	return &realQuery{q: r.sess.Query(stmt, values...)}
}

func (r *realSession) Close() {
	r.sess.Close()
}

// Adapter for real gocql.Query
type realQuery struct {
	q *gocql.Query
}

func (rq *realQuery) Exec() error {
	return rq.q.Exec()
}

func (rq *realQuery) MapScan(dest map[string]interface{}) error {
	return rq.q.MapScan(dest)
}

// add Consistency and MapScanCAS method to realQuery (as needed for some operations below)
func (rq *realQuery) Consistency(c gocql.Consistency) *realQuery {
	rq.q = rq.q.Consistency(c)
	return rq
}

func (rq *realQuery) MapScanCAS(dest map[string]interface{}) (bool, error) {
	return rq.q.MapScanCAS(dest)
}

// NewCassandraClient creates a new session
func NewCassandraClient(hosts []string, keyspace string) (*CassandraClient, error) {
	// create a new Cassandra cluster
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = 5 * time.Second

	// create a cluster session
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	return &CassandraClient{session: &realSession{sess}}, nil
}

// close the cassandra session
func (c *CassandraClient) Close() {
	if c.session != nil {
		// Only realSession needs Close method to be called
		if rs, ok := c.session.(*realSession); ok {
			rs.Close()
		}
	}
}

// ApplyChange applies idempotent sink of a Debezium change event to cassandra
// for supported debezium topics.`topic` is the Kafka topic name (e.g. "cdc.public.users")
func (c *CassandraClient) ApplyChange(topic string, ev *model.ChangeEvent) error {
	if ev == nil {
		logger.DebugLogger.Println("nil change event")
		return nil
	}

	// deduplicate using processed_events table (INSERT IF NOT EXISTS)
	applied, err := c.addEventIfNotProcessed(ev.EventID, topic, ev.TsMs)
	if err != nil {
		return fmt.Errorf("addEventIfNotProcessed: %w", err)
	}
	if !applied {
		// already processed
		return nil
	}

	// route by topic suffix (table name)
	switch topic {
	case config.DebeziumUsersTopic:
		return c.applyUserChange(ev)
	case config.DebeziumOrdersTopic:
		return c.applyOrderChange(ev)
	default:
		return fmt.Errorf("unknown Debezium topic: %s", topic)
	}
}

// addEventIfNotProcessed returns true if the event was newly added (i.e., not seen before)
func (c *CassandraClient) addEventIfNotProcessed(eventID string, topic string, tsMs int64) (bool, error) {

	logger.DebugLogger.Printf("Inserting event to processed_events table: event_id = %s\n", eventID)

	// LWT insert: INSERT ... IF NOT EXISTS
	query := "INSERT INTO processed_events (event_id, topic, ts_ms, processed_at) VALUES (?, ?, ?, ?) IF NOT EXISTS"

	// Type assertion to access Consistency and MapScanCAS for realSession
	if rs, ok := c.session.(*realSession); ok {
		q := rs.sess.Query(query, eventID, topic, tsMs, time.Now()).Consistency(gocql.Quorum)
		applied, err := q.MapScanCAS(make(map[string]interface{})) // applied = true if the row insertion was successful
		if err != nil {
			return false, err
		}

		logger.DebugLogger.Printf("Row already existed? %v", !applied)
		return applied, nil
	}
	// For mocks, just return true
	return true, nil
}

func (c *CassandraClient) applyUserChange(ev *model.ChangeEvent) error {
	row := ev.Row
	if row == nil {
		return nil
	}

	logger.DebugLogger.Printf("Applying change event: Table = 'users', Op = '%s', Row = %v\n", ev.Op, row)

	if ev.Op != "c" && ev.Op != "u" {
		// nothing to do
		logger.InfoLogger.Printf("ApplyChange: unexpected op '%s', nothing to do.\n", ev.Op)
		return nil
	}

	id, err := convertJsonValueToCqlUUID(row["id"])
	if err != nil {
		return err
	}

	// extract necessary fields from the row
	name := row["name"].(string)

	dobInt := parser.ParseDebeziumNumber[int32](row["dob"])
	dobTime := parseDebeziumDate(dobInt)
	dob := dobTime.Format("2006-01-02") // extract date only as a string

	// the timestamps in Debezium are in RFC3339 formatted string
	createdAt, _ := time.Parse(time.RFC3339, row["created_at"].(string))

	switch ev.Op {

	case "c":
		stmt := "INSERT INTO users (id, name, dob, created_at, is_deleted) VALUES (?, ?, ?, ?, ?)"
		return c.session.Query(stmt, id, name, dob, createdAt, false).Exec()

	case "u":
		modifiedAt, _ := time.Parse(time.RFC3339, row["modified_at"].(string))
		isDeleted, _ := row["is_deleted"].(bool)

		// if it's a deleting update, just set the deleted_ind (without updating the name)
		if isDeleted {
			stmt := "UPDATE users SET modified_at = ?, is_deleted = ? WHERE id = ? and name = ?"
			return c.session.Query(stmt, modifiedAt, true, id, name).Exec()
		} else {
			// read the old record's name, insert updated record and delete the old record if old name is different

			// SELECT
			var selectRow = map[string]interface{}{}
			readStmt := "SELECT name from users where id = ? LIMIT 1"
			err := c.session.Query(readStmt, id).MapScan(selectRow)
			if err != nil {
				return fmt.Errorf("applyUserChange: error when reading existing record with user id %v: %w", id, err)
			}

			var currName string
			if _, ok := c.session.(*realSession); ok {
				res, ok := selectRow["name"]
				if !ok {
					return fmt.Errorf("applyUserChange: error when reading existing record with user id %v: 'name' not found in the returned column set", id)
				}
				currName = res.(string)
			}

			// INSERT
			insertStmt := "INSERT INTO users (id, name, dob, created_at, modified_at, is_deleted) VALUES (?, ?, ?, ?, ?, ?)"
			err = c.session.Query(insertStmt, id, name, dob, createdAt, modifiedAt, false).Exec()
			if err != nil {
				return fmt.Errorf("applyUserChange: error when inserting updated record for user id %v: %w", id, err)
			}

			// DELETE old record if the name is different
			// (If the updated name is not different, then Cassandra will automatically update the existing record
			//  due to having the same clustering key column (i.e. name) value)
			if name != currName {
				deleteStmt := "DELETE FROM users WHERE id = ? and name = ?"
				return c.session.Query(deleteStmt, id, currName).Exec()
			}
		}
	}

	return nil
}

func (c *CassandraClient) applyOrderChange(ev *model.ChangeEvent) error {
	row := ev.Row
	if row == nil {
		return nil
	}

	logger.DebugLogger.Printf("Applying change event: Table = 'orders', Op = '%s', Row = %v\n", ev.Op, row)

	if ev.Op != "c" && ev.Op != "u" {
		// nothing to do
		logger.InfoLogger.Printf("ApplyChange: unexpected op '%s', nothing to do.\n", ev.Op)
		return nil
	}

	orderID, err := convertJsonValueToCqlUUID(row["id"])
	if err != nil {
		return err
	}

	userID, err := convertJsonValueToCqlUUID(row["user_id"])
	if err != nil {
		return err
	}

	status := row["status"].(string)
	quantity := parser.ParseDebeziumNumber[int](row["quantity"])
	total := debeziumDecimalToInfDec(row["total_amount"].(string), 2)

	switch ev.Op {
	case "c":
		// the timestamps are RFC3339 formatted string so just pass as is to cassandra
		placedAt, _ := time.Parse(time.RFC3339, row["placed_at"].(string))
		// insert into orders and orders_by_user
		q1 := `INSERT INTO orders (order_id, user_id, status, quantity, total_amount, placed_at, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`
		if err := c.session.Query(q1, orderID, userID, status, quantity, total, placedAt, false).Exec(); err != nil {
			return err
		}
		q2 := `INSERT INTO orders_by_user (user_id, order_id, status, quantity, total_amount, placed_at, is_deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`
		return c.session.Query(q2, userID, orderID, status, quantity, total, placedAt, false).Exec()
	case "u":
		modifiedAt, _ := time.Parse(time.RFC3339, row["modified_at"].(string))
		isDeleted := row["is_deleted"].(bool)
		q1 := `UPDATE orders SET status = ?, modified_at = ?, is_deleted = ? WHERE order_id = ? AND user_id = ?`
		if err := c.session.Query(q1, status, modifiedAt, isDeleted, orderID, userID).Exec(); err != nil {
			return err
		}
		q2 := `UPDATE orders_by_user SET status = ?, modified_at = ?, is_deleted = ? WHERE user_id = ? AND order_id = ?`
		return c.session.Query(q2, status, modifiedAt, isDeleted, userID, orderID).Exec()
	}

	return nil
}

func parseDebeziumDate(days int32) time.Time {
	// Debezium date = days since epoch (1970-01-01, UTC)
	epoch := time.Unix(0, 0).UTC()
	return epoch.AddDate(0, 0, int(days))
}

// helper function to convert an interface{} value to a valid CQL UUID
func convertJsonValueToCqlUUID(idVal any) (gocql.UUID, error) {
	idStr, ok := idVal.(string)
	if !ok {
		return gocql.UUID{}, fmt.Errorf("applyUserChange: 'id' must be a string value")
	}
	uuid, err := gocql.ParseUUID(idStr)
	if err != nil {
		return gocql.UUID{}, fmt.Errorf("applyUserChange: invalid UUID format: %w", err)
	}
	return uuid, nil
}

// parse debezium decimal number with given scale (encoded as a base64 string)
// to inf.Dec type - the type required by CQL to accept a decimal column value
func debeziumDecimalToInfDec(b64 string, scale int) *inf.Dec {
	// Step 1: Base64 decode
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		panic(err)
	}

	// Step 2: Interpret bytes as two’s complement integer
	i := new(big.Int)
	if len(raw) > 0 && raw[0]&0x80 != 0 { // negative number
		// two's complement conversion
		tmp := make([]byte, len(raw))
		for j := range raw {
			tmp[j] = ^raw[j]
		}
		i.SetBytes(tmp)
		i.Add(i, big.NewInt(1))
		i.Neg(i)
	} else {
		i.SetBytes(raw)
	}

	// Step 3: create inf.Dec with scale
	return inf.NewDecBig(i, inf.Scale(scale))
}
