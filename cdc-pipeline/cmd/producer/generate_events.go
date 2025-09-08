package main

// a test program to generate and show generated events
// uncomment below function run program in this file

// func main() {
// 	const NUM_EVENTS int = 7
// 	userEvents := eventgenerator.GenerateRandomUserEvents(NUM_EVENTS)

// 	// serialise events to a json object
// 	jsonUserEvents, err := json.Marshal(userEvents)
// 	if err != nil {
// 		fmt.Printf("error marshalling user events: %v\n", err)
// 		return
// 	}
// 	fmt.Printf("%s\n", string(jsonUserEvents))
// 	fmt.Println()

// 	var userIds []model.UUID
// 	for _, e := range userEvents {
// 		userIds = append(userIds, e.UserId)
// 	}

// 	orderEvents := eventgenerator.GenerateRandomOrderEvents(NUM_EVENTS, userIds)

// 	// serialise events to a json object
// 	jsonOrderEvents, err := json.Marshal(orderEvents)
// 	if err != nil {
// 		fmt.Printf("error marshalling order events: %v\n", err)
// 		return
// 	}
// 	fmt.Printf("%s\n", string(jsonOrderEvents))
// 	fmt.Println()
// }
