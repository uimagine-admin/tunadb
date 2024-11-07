package db

import (
	"os"
	"testing"

	"github.com/uimagine-admin/tunadb/internal/types"
)

func TestHandleInsert(t *testing.T) {
	// Create a temporary JSON file for testing
	filename := "../data/test_sample_insert.json"
	originalFilename := "../data/sample.json"

	// Copy the original sample.json to test_sample_insert.json
	input, err := os.ReadFile(originalFilename)
	if err != nil {
		t.Fatalf("Failed to read original sample.json: %s", err)
	}

	err = os.WriteFile(filename, input, 0644)
	if err != nil {
		t.Fatalf("Failed to create test JSON file: %s", err)
	}
	// defer os.Remove(filename)

	// Initialize a Handler instance
	handler := &Handler{
		Node: &types.Node{
			ID: "test_sample_insert",
			// Initialize other Node fields as necessary
		},
	}

	// Define the parameters for the HandleInsert method
	tableName := "sample"
	partitionKey := int64(123123)
	partitionKeyValues := []string{"1", "Specialized"}
	newCells := []Cell{
		{Name: "New Cell", Value: "New Value"},
	}

	// Call HandleInsert method
	err = handler.HandleInsert(tableName, partitionKey, partitionKeyValues, newCells)
	if err != nil {
		t.Errorf("HandleInsert failed: %s", err)
	} else {
		t.Logf("HandleInsert succeeded")
	}

	// Verify the results
	localData, err := ReadJSON(filename)
	if err != nil {
		t.Fatalf("Failed to read JSON: %s", err)
	}

	table := GetTable(tableName, localData)
	if table == nil {
		t.Errorf("Table %s not found", tableName)
	} else {
		partition := GetPartition(table, partitionKey)
		if partition == nil {
			t.Errorf("Partition with key %d not found", partitionKey)
		} else {
			found := false
			for _, row := range partition.Rows {
				for _, cell := range row.Cells {
					if cell.Name == "New Cell" && cell.Value == "New Value" {
						found = true
						break
					}
				}
				if found {
					break
				}
			}
			if !found {
				t.Errorf("New cell not found in partition")
			} else {
				t.Logf("New cell successfully inserted into partition")
			}
		}
	}
}
