package db

import (
	"testing"
)

func TestReadJSON(t *testing.T) {
	// Create a temporary JSON file for testing
	filename := "../data/sample.json" // change this to whatever suits u

	// Test ReadJSON function
	data, err := ReadJSON(filename)
	if err != nil {
		t.Errorf("ReadJSON failed: %s", err)
	}
	if len(data) != 1 || data[0].TableName != "timestamp" {
		t.Errorf("ReadJSON returned unexpected data: %s", data)
	} else {
		t.Logf("ReadJSON succeeded: %s", data)
	}
}

func TestCheckTableExists(t *testing.T) {
	filename := "../data/sample.json" // change this to whatever suits u

	data, err := ReadJSON(filename)

	t.Logf("ReadJSON retrieved: %s, %s", data, err)

	if !CheckTableExists("sample", data) {
		t.Errorf("CheckTableExists failed to find existing table")
	}
	if CheckTableExists("other-name", data) {
		t.Errorf("CheckTableExists found non-existent table")
	}
}
