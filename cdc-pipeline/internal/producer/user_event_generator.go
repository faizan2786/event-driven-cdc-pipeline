package producer

import (
	"math"
	"math/rand"
	"time"

	"github.com/faizan2786/system-design/cdc-pipeline/internal/model"
	"github.com/google/uuid"
)

// define a list for storing generated uuids (useful for UPDATE and DELETE events)

var myUserIDs []string

//var myOrderIDs []string

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randomName() string {
	firstLen := rand.Intn(5) + 3 // 3-7 chars
	lastLen := rand.Intn(5) + 3  // 3-7 chars
	firstName := randomString(firstLen)
	lastName := randomString(lastLen)
	return firstName + " " + lastName
}

func randomDOB() model.Date {
	year := rand.Intn(2015-1970+1) + 1970 // 1970 to 2015
	month := time.Month(rand.Intn(12) + 1)
	day := rand.Intn(28) + 1 // To avoid invalid dates
	return model.Date(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}

func generateUserEvent(eventType model.EventType, userId string) model.UserEvent {

	var event model.UserEvent
	currentTime := model.DateTime(time.Now())

	switch eventType {
	case model.CREATE:
		dob := randomDOB()
		name := randomName()
		event = model.UserEvent{
			Type:      eventType,
			UserId:    model.UUID(userId),
			Name:      name,
			DOB:       &dob,
			CreatedAt: &currentTime,
		}
	case model.UPDATE:
		event = model.UserEvent{
			Type:       eventType,
			UserId:     model.UUID(userId),
			Name:       randomName(), // Optionally randomize name on update
			ModifiedAt: &currentTime,
		}
	case model.DELETE:
		event = model.UserEvent{
			Type:       eventType,
			UserId:     model.UUID(userId),
			ModifiedAt: &currentTime,
		}
	default:
		panic("Unknown Event type")
	}

	return event
}

// return random n number of user events
func GenerateRandomUserEvents(n int) []model.UserEvent {

	var generatedEvents []model.UserEvent

	// generate around
	// 70% create events
	// 20% update events
	// 10% delete events

	// CREATE
	numCreateEvents := int(math.Round(0.6 * float64(n)))

	for i := 0; i < numCreateEvents; i++ {
		id := uuid.New().String()
		myUserIDs = append(myUserIDs, id) // store the id in the list
		generatedEvents = append(generatedEvents, generateUserEvent(model.CREATE, id))
	}

	// UPDATE
	numUpdateEvents := int(math.Round(0.3 * float64(n)))
	for i := 0; i < numUpdateEvents; i++ {
		if len(myUserIDs) == 0 {
			break
		}
		randInt := rand.Intn(len(myUserIDs))
		id := myUserIDs[randInt]
		generatedEvents = append(generatedEvents, generateUserEvent(model.UPDATE, id))
	}

	// DELETE
	numDeleteEvents := n - (numCreateEvents + numUpdateEvents)
	for i := 0; i < numDeleteEvents; i++ {
		if len(myUserIDs) == 0 {
			break
		}
		randInt := rand.Intn(len(myUserIDs))
		id := myUserIDs[randInt]
		generatedEvents = append(generatedEvents, generateUserEvent(model.DELETE, id))
	}

	return generatedEvents
}
