package db

import (
	"os"
	"testing"

	"github.com/uimagine-admin/tunadb/internal/types"
)

func TestHandleRead(t *testing.T) {
	// Create a temporary JSON file for testing
	filename := "../data/test_sample.json"
	originalFilename := "../data/sample.json"

	// Copy the original sample.json to test_sample.json
	input, err := os.ReadFile(originalFilename)
	if err != nil {
		t.Fatalf("Failed to read original sample.json: %s", err)
	}

	err = os.WriteFile(filename, input, 0644)
	if err != nil {
		t.Fatalf("Failed to create test JSON file: %s", err)
	}
	defer os.Remove(filename)

	// Initialize a Handler instance
	handler := &Handler{
		Node: &types.Node{
			// Initialize the Node fields as necessary
			IPAddress: "ip-addr",
			ID:        "bleh123",
			Port:      3000,
			Name:      "node-bleh123",
		},
	}

	// Define test parameters
	tableName := "sample"
	partitionKey := int64(123123)

	// Call HandleRead method
	response, err := handler.HandleRead(filename, tableName, partitionKey)
	if err != nil {
		t.Errorf("HandleRead failed: %s", err)
	} else {
		t.Logf("HandleRead succeeded: %+v", response)
	}
}
