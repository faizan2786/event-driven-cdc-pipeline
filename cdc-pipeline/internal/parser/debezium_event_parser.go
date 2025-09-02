package parser

import (
	"encoding/json"
	"fmt"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
)

// ParseDebeziumEvent parses a Debezium JSON envelope (value) into model.ChangeEvent object.
// The project uses JSON converter (no Avro/schema-registry).
func ParseDebeziumEvent(value []byte) (*model.ChangeEvent, error) {
	var envelope model.JsonMap
	if err := json.Unmarshal(value, &envelope); err != nil {
		return nil, err
	}
	payloadRaw, ok := envelope["payload"]
	if !ok || payloadRaw == nil {
		return nil, fmt.Errorf("missing payload in debezium message")
	}
	payload, ok := payloadRaw.(model.JsonMap) // check if payloadRaw is a valid json and load it as a map
	if !ok {
		return nil, fmt.Errorf("invalid payload format")
	}

	ev := &model.ChangeEvent{}

	if op, ok := payload["op"].(string); ok {
		ev.Op = op
	}
	if after, ok := payload["after"].(model.JsonMap); ok {
		ev.Row = after
	}
	ev.TsMs = ParseDebeziumNumber[int64](payload["ts_ms"])

	eventID, err := constructEventID(payload, ev.TsMs)
	if err != nil {
		return nil, err
	}
	ev.EventID = eventID

	return ev, nil
}

// Build a stable EventID using common identifiers in the source fields (txId, lsn and ts_us)
func constructEventID(payload model.JsonMap, tsMs int64) (string, error) {

	// extract source field from the payload
	source, ok := payload["source"].(model.JsonMap)
	if !ok || source == nil {
		return "", fmt.Errorf("missing 'source' field in the payload")
	}

	var idParts []any

	if v, ok := source["txId"]; ok {
		idParts = append(idParts, v)
	}
	if v, ok := source["lsn"]; ok { // i.e. a Log Sequence Number
		idParts = append(idParts, v)
	}
	if v, ok := source["ts_us"]; ok {
		idParts = append(idParts, v)
	}

	// fallback to tsMs if above fields are absent
	if len(idParts) == 0 {
		idParts = append(idParts, tsMs)
	}

	// marshal idParts to json string
	idBytes, _ := json.Marshal(idParts)
	return string(idBytes), nil
}

// convert debezium number (i.e. an int value) based on its type and return as type T
func ParseDebeziumNumber[T int | int32 | int64](numberStr interface{}) T {
	// use type switch to infer correct type of the json value
	switch v := numberStr.(type) {
	case float64:
		return T(v)
	case int:
		return T(v)
	case int32:
		return T(v)
	case int64:
		return T(v)
	default:
		panic("ParseDebeziumNumber: unsupported numeric type")
	}
}
