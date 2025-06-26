package consumer

import "testing"

func TestConnToDB(t *testing.T) {
	_, err := ConnectToDB()
	if err != nil {
		t.Errorf("DB connection was not successful.\n%v", err)
	}
}
