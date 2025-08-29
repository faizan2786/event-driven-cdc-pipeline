package model

type JsonMap = map[string]any // a map of strings with value of any types (used to represent a json object as a map)

// ChangeEvent is a small normalized representation of a Debezium change event
type ChangeEvent struct {
	Op   string  // operation: "c","u" "d","r" (read snapshot) - only "c" and "u" changes are relevant
	Row  JsonMap // the data row after the change (nil for deletes)
	TsMs int64   // event timestamp ms
	// EventID is a stable unique id for this change (derived from source metadata)
	EventID string
}
