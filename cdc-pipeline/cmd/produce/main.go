package main

import (
	"encoding/json"
	"fmt"

	"github.com/faizan2786/system-design/cdc-pipeline/internal/producer"
)

func main() {

	events := producer.GenerateRandomUserEvents(7)

	// serialise events to a json object
	jsonEvents, err := json.Marshal(events)
	if err != nil {
		fmt.Printf("error marshalling events: %v\n", err)
		return
	}

	fmt.Printf("%s", string(jsonEvents))
}
