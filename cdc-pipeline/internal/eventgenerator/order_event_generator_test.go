package eventgenerator

import (
	"encoding/json"
	"testing"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/model"
	"github.com/google/uuid"
)

func generateRandomUserIDs() ([]model.UUID, map[model.UUID]bool) {
	userIDs := make([]model.UUID, 0)
	userIDMap := make(map[model.UUID]bool)
	for i := 0; i < 10; i++ {
		id := model.UUID(uuid.New().String())
		userIDs = append(userIDs, id)
		userIDMap[id] = true
	}
	return userIDs, userIDMap
}

func TestGenerateRandomOrderEvents(t *testing.T) {
	myUserIDs, myUserIDSet := generateRandomUserIDs()

	n := 10
	events := GenerateRandomOrderEvents(n, myUserIDs)
	if len(events) != n {
		t.Errorf("expected %d events, got %d", n, len(events))
	}

	var createCount, updateCount, deleteCount int
	myOrderIDs := make(map[string]bool) // keep track of created user ids

	for _, e := range events {
		switch e.Type {
		case model.CREATE:
			createCount++
			if e.OrderId == "" {
				t.Error("CREATE event has empty OrderId")
			}
			if !myUserIDSet[e.UserId] {
				t.Error("CREATE event has an unknown UserId")
			}
			if e.Status != model.PLACED {
				t.Errorf("CREATE event should have order status = PLACED, got %v", e.Status)
			}

			// check for required fields
			if e.Quantity == 0 || e.OrderTotal == 0 || e.PlacedAt == nil {
				jsonEvent, _ := json.Marshal(e)
				t.Errorf("CREATE event missing field(s): got, %v", string(jsonEvent))
			}
			// check for omitted fields
			if e.ModifiedAt != nil {
				t.Errorf("CREATE event should not have ModifiedAt field, got: %v", e.ModifiedAt)
			}

			myOrderIDs[string(e.OrderId)] = true // add id to the user id set

		case model.UPDATE:
			updateCount++
			if e.OrderId == "" {
				t.Error("UPDATE event has empty OrderId")
			}
			// check if order id is present in created order ids
			if !myOrderIDs[string(e.OrderId)] {
				t.Errorf("UPDATE event OrderId not found in CREATE events: %s", e.OrderId)
			}
			if e.Status != model.SHIPPED {
				t.Errorf("UPDATE event should have order status = SHIPPED, got %v", e.Status)
			}

			// check for required fields
			if e.ModifiedAt == nil {
				t.Error("UPDATE event missing ModifiedAt")
			}
			// check for any omitted fields
			if e.UserId != "" || e.Quantity != 0 || e.OrderTotal != 0 || e.PlacedAt != nil {
				jsonEvent, _ := json.Marshal(e)
				t.Errorf("UPDATE event contains omitted field(s): got, %v", string(jsonEvent))
			}

		case model.DELETE:
			deleteCount++
			if e.OrderId == "" {
				t.Error("DELETE event has empty OrderId")
			}
			// check if order id is present in created order ids
			if !myOrderIDs[string(e.OrderId)] {
				t.Errorf("DELETE event OrderId not found in CREATE events: %s", e.OrderId)
			}
			if e.Status != model.CANCELLED {
				t.Errorf("DELETE event should have order status = CANCELLED, got %v", e.Status)
			}

			// check for required fields

			// check for required fields
			if e.ModifiedAt == nil {
				t.Error("DELETE event missing ModifiedAt")
			}
			// check for any omitted fields
			if e.UserId != "" || e.Quantity != 0 || e.OrderTotal != 0 || e.PlacedAt != nil {
				jsonEvent, _ := json.Marshal(e)
				t.Errorf("DELETE event contains omitted field(s): got, %v", string(jsonEvent))
			}
		}
	}

	if createCount == 0 || updateCount == 0 || deleteCount == 0 {
		t.Errorf("expected some of each event type, got CREATE: %d, UPDATE: %d, DELETE: %d", createCount, updateCount, deleteCount)
	}
}
