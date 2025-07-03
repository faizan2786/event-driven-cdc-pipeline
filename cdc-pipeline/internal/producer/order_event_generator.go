package producer

import (
	"math"
	"math/rand"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	"github.com/google/uuid"
)

// define a list for storing generated uuids (useful for UPDATE and DELETE events)
var myOrderIDs []string

func generateOrderEvent(eventType model.EventType, orderId string, userIds []model.UUID) model.OrderEvent {

	var event model.OrderEvent
	currentTime := model.DateTime(time.Now())

	switch eventType {
	case model.CREATE:
		quantity := rand.Intn(100) + 1
		event = model.OrderEvent{
			Type:       eventType,
			OrderId:    model.UUID(orderId),
			Status:     model.PLACED,
			UserId:     userIds[rand.Intn(len(userIds))],
			Quantity:   quantity,
			OrderTotal: (rand.Float64()*50 + 1) * float64(quantity),
			PlacedAt:   &currentTime,
		}
	case model.UPDATE:
		event = model.OrderEvent{
			Type:       eventType,
			OrderId:    model.UUID(orderId),
			Status:     model.SHIPPED,
			ModifiedAt: &currentTime,
		}
	case model.DELETE:
		event = model.OrderEvent{
			Type:       eventType,
			OrderId:    model.UUID(orderId),
			Status:     model.CANCELLED,
			ModifiedAt: &currentTime,
		}
	default:
		panic("unknown event type")
	}

	return event
}

// return random n number of user events
func GenerateRandomOrderEvents(n int, userIds []model.UUID) []model.OrderEvent {

	var generatedEvents []model.OrderEvent

	// generate around
	// 70% create events
	// 20% update events
	// 10% delete events

	// CREATE
	numCreateEvents := int(math.Round(0.6 * float64(n)))

	for i := 0; i < numCreateEvents; i++ {
		id := uuid.New().String()
		myOrderIDs = append(myOrderIDs, id) // store the id in the list
		generatedEvents = append(generatedEvents, generateOrderEvent(model.CREATE, id, userIds))
	}

	// UPDATE
	numUpdateEvents := int(math.Round(0.3 * float64(n)))
	for i := 0; i < numUpdateEvents; i++ {
		if len(myOrderIDs) == 0 {
			break
		}
		randInt := rand.Intn(len(myOrderIDs))
		id := myOrderIDs[randInt]
		generatedEvents = append(generatedEvents, generateOrderEvent(model.UPDATE, id, userIds))
	}

	// DELETE
	numDeleteEvents := n - (numCreateEvents + numUpdateEvents)
	for i := 0; i < numDeleteEvents; i++ {
		if len(myOrderIDs) == 0 {
			break
		}
		randInt := rand.Intn(len(myOrderIDs))
		id := myOrderIDs[randInt]
		generatedEvents = append(generatedEvents, generateOrderEvent(model.DELETE, id, userIds))
	}

	return generatedEvents
}
