package main

import (
	"encoding/json"
	"fmt"

	"github.com/faizan2786/system-design/cdc-pipeline/internal/model"
	"github.com/faizan2786/system-design/cdc-pipeline/internal/producer"
)

// change to main to run the program in the file
func main_test() {
	const NUM_EVENTS int = 7
	userEvents := producer.GenerateRandomUserEvents(NUM_EVENTS)

	// serialise events to a json object
	jsonUserEvents, err := json.Marshal(userEvents)
	if err != nil {
		fmt.Printf("error marshalling user events: %v\n", err)
		return
	}
	fmt.Printf("%s\n", string(jsonUserEvents))
	fmt.Println()

	var userIds []model.UUID
	for _, e := range userEvents {
		userIds = append(userIds, e.UserId)
	}

	orderEvents := producer.GenerateRandomOrderEvents(NUM_EVENTS, userIds)

	// serialise events to a json object
	jsonOrderEvents, err := json.Marshal(orderEvents)
	if err != nil {
		fmt.Printf("error marshalling order events: %v\n", err)
		return
	}
	fmt.Printf("%s\n", string(jsonOrderEvents))
	fmt.Println()
}
