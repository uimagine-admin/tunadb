package db

import (
	"encoding/json"
	"os"
	"testing"

	pb "github.com/uimagine-admin/tunadb/api"
)

func TestHandleDelete(t *testing.T) {
	nodeId := "test_node"
	filename := "./internal/data/" + nodeId + ".json"

	// Setup: Create test data
	initialRows := []Row{
		{PageId: "1", ComponentId: "button1", Event: "click", Timestamp: "2023-11-26T12:00:00Z"},
		{PageId: "1", ComponentId: "button2", Event: "hover", Timestamp: "2023-11-26T12:01:00Z"},
		{PageId: "2", ComponentId: "link1", Event: "click", Timestamp: "2023-11-26T12:02:00Z"},
	}

	os.MkdirAll("./internal/data", os.ModePerm)
	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("Failed to create test data file: %v", err)
	}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(initialRows); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	file.Close()

	// Test deletion
	req := &pb.DeleteRequest{
		PageId:      "1",
		ComponentId: "button1",
	}

	err = HandleDelete(nodeId, req)
	if err != nil {
		t.Fatalf("HandleDelete returned error: %v", err)
	}

	// Verify the data
	file, err = os.Open(filename)
	if err != nil {
		t.Fatalf("Failed to open data file: %v", err)
	}
	defer file.Close()

	var updatedRows []Row
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&updatedRows); err != nil {
		t.Fatalf("Failed to decode data: %v", err)
	}

	if len(updatedRows) != 2 {
		t.Fatalf("Expected 2 rows after deletion, got %d", len(updatedRows))
	}

	// Clean up
	os.Remove(filename)
}
