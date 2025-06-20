package model

import (
	"time"
)

// type aliases
type UUID = string
type DateTime = time.Time

// custom date type with json marshalling
type Date time.Time

func (dt Date) MarshalJSON() ([]byte, error) {
	t := time.Time(dt)
	return []byte(`"` + t.Format("2006-01-02") + `"`), nil
}

func (dt *Date) UnmarshalJSON(data []byte) error {
	// Remove quotes
	str := string(data)
	if len(str) < 2 {
		return nil
	}
	str = str[1 : len(str)-1]
	parsed, err := time.Parse("2006-01-02", str)
	if err != nil {
		return err
	}
	*dt = Date(parsed)
	return nil
}

// enums...

type EventType string

const (
	CREATE EventType = "CREATE"
	UPDATE EventType = "UPDATE"
	DELETE EventType = "DELETE"
)

type OrderStatus string

const (
	PLACED    OrderStatus = "PLACED"
	CANCELLED OrderStatus = "CANCELLED"
	SHIPPED   OrderStatus = "SHIPPED"
)

// events...

// NOTE: omitempty tag doesn't work for time.Time types directly.
// Hence, a pointer type is used for date types

type UserEvent struct {
	Type       EventType `json:"event_type"`
	UserId     UUID      `json:"user_id"`
	Name       string    `json:"name,omitempty"`
	DOB        *Date     `json:"dob,omitempty"`
	CreatedAt  *DateTime `json:"created_at,omitempty"`
	ModifiedAt *DateTime `json:"modified_at,omitempty"`
}

type OrderEvent struct {
	Type       EventType   `json:"event_type"`
	OrderId    UUID        `json:"order_id"`
	Status     OrderStatus `json:"order_status"`
	UserId     UUID        `json:"user_id,omitempty"`
	Quantity   int         `json:"quantity,omitempty"`
	OrderTotal float64     `json:"order_total,omitempty"`
	PlacedAt   *DateTime   `json:"placed_at,omitempty"`
	ModifiedAt *DateTime   `json:"modified_at,omitempty"`
}
