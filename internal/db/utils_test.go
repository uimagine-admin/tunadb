package db

import (
	"testing"
)

func TestReadJSON(t *testing.T) {
	// Create a temporary JSON file for testing
	filename := "../data/sample.json"

	// Test ReadJSON function
	data, err := ReadJSON(filename)
	if err != nil {
		t.Errorf("ReadJSON failed: %s", err)
	}
	if len(data) != 1 || data[0].TableName != "timestamp" {
		t.Errorf("ReadJSON returned unexpected data: %+v", data)
	} else {
		t.Logf("ReadJSON succeeded: %+v", data)
	}
}
