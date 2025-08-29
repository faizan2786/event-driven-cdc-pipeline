package parser

import (
	"testing"
)

func TestParseDebeziumEvent_BasicParsing(t *testing.T) {
	// Example Debezium event (insert op)
	event := []byte(
		`{
			"schema": {
				"type": "struct",
				"fields": []
			},
			"payload": {
				"op": "c",
				"before": null,
				"after": {"id": "28822318-1dde-4cf6-b9d3-62dec8def32c", "name": "Alice"},
				"ts_ms": 1690000000000,
				"source": {
					"txId": "tx123",
					"lsn": 456,
					"ts_us": 1690000000000123
				}
			}
   		}`)

	ce, err := ParseDebeziumEvent(event)
	if err != nil {
		t.Fatalf("ParseDebeziumEvent failed: %v", err)
	}

	if ce.Op != "c" {
		t.Errorf("expected Op 'c', got '%s'", ce.Op)
	}
	if ce.TsMs != 1690000000000 {
		t.Errorf("expected TsMs 1690000000000, got %d", ce.TsMs)
	}
	if ce.Row["id"] != "28822318-1dde-4cf6-b9d3-62dec8def32c" || ce.Row["name"] != "Alice" {
		t.Errorf("unexpected Row: %+v", ce.Row)
	}
	if ce.EventID == "" {
		t.Errorf("EventID should not be empty")
	}
}
