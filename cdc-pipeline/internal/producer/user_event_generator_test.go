package producer

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/faizan2786/system-design/cdc-pipeline/internal/model"
)

func TestGenerateRandomUserEvents(t *testing.T) {
	n := 10
	events := GenerateRandomUserEvents(n)
	if len(events) != n {
		t.Errorf("expected %d events, got %d", n, len(events))
	}

	var createCount, updateCount, deleteCount int
	userIDs := make(map[string]bool) // keep track of created user ids

	for _, e := range events {
		switch e.Type {
		case model.CREATE:
			createCount++
			if e.UserId == "" {
				t.Error("CREATE event has empty UserId")
			}
			if e.DOB == nil || e.CreatedAt == nil {
				t.Error("CREATE event missing DOB or CreatedAt")
			}
			// ModifiedAt should be nil for CREATE events
			if e.ModifiedAt != nil {
				t.Errorf("CREATE event should not have ModifiedAt field, got: %v", e.ModifiedAt)
			}

			userIDs[string(e.UserId)] = true // add id to the user id set

		case model.UPDATE:
			updateCount++
			if e.UserId == "" {
				t.Error("UPDATE event has empty UserId")
			}
			if e.ModifiedAt == nil {
				t.Error("UPDATE event missing ModifiedAt")
			}
			// DOB and CreatedAt should be nil for CREATE events
			if e.CreatedAt != nil || e.DOB != nil {
				t.Errorf("UPDATE event should not have CreatedAt and DOB fields, got CreatedAt: %v, DOB: %v", e.CreatedAt, e.DOB)
			}

			// check if user id is present in created user id set
			if !userIDs[string(e.UserId)] {
				t.Errorf("UPDATE event UserId not found in CREATE events: %s", e.UserId)
			}

		case model.DELETE:
			deleteCount++
			if e.UserId == "" {
				t.Error("DELETE event has empty UserId")
			}
			if e.ModifiedAt == nil {
				t.Error("DELETE event missing ModifiedAt")
			}

			// check if user id is present in created user id set
			if !userIDs[string(e.UserId)] {
				t.Errorf("DELETE event UserId not found in CREATE events: %s", e.UserId)
			}

			// DOB and CreatedAt should be nil for CREATE events
			if e.CreatedAt != nil || e.DOB != nil || e.Name != "" {
				jsonEvent, _ := json.Marshal(e)
				t.Errorf("DELETE event should only have even Type, ModifiedAt and UserId fields, got %v", string(jsonEvent))
			}
		}
	}

	if createCount == 0 || updateCount == 0 || deleteCount == 0 {
		t.Errorf("expected some of each event type, got CREATE: %d, UPDATE: %d, DELETE: %d", createCount, updateCount, deleteCount)
	}
}

func TestRandomName(t *testing.T) {
	for i := 0; i < 100; i++ {
		name := randomName()
		parts := strings.Split(name, " ")
		if len(parts) != 2 {
			t.Errorf("Expected two parts in name, got: %v", name)
		}
		for _, part := range parts {
			if len(part) < 3 || len(part) > 7 {
				t.Errorf("Name part length out of range: %v", part)
			}
			if !regexp.MustCompile(`^[a-z]+$`).MatchString(part) {
				t.Errorf("Name part contains invalid characters: %v", part)
			}
		}
	}
}

func TestRandomDOB(t *testing.T) {
	for i := 0; i < 100; i++ {
		dob := time.Time(randomDOB())
		if dob.Year() < 1970 || dob.Year() > 2015 {
			t.Errorf("DOB year out of range: %v", dob.Year())
		}
		if dob.Day() < 1 || dob.Day() > 28 {
			t.Errorf("DOB day out of range: %v", dob.Day())
		}
	}
}

func TestCreateUserEvent(t *testing.T) {
	userId := "test-user-id"
	event := generateUserEvent(model.CREATE, userId)
	if event.Name == "" {
		t.Error("Name should not be empty")
	}
	if event.DOB == nil {
		t.Error("DOB should not be nil")
	}
	if event.CreatedAt == nil {
		t.Error("CreatedAt should not be nil")
	}
}
