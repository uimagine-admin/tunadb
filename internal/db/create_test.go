package db

import (
	"os"
	"testing"
	"time"

	"github.com/uimagine-admin/tunadb/internal/types"
)

func TestHandleCreate(t *testing.T) {
	// Create a temporary JSON file for testing
	filename := "../data/test_sample_create.json"
	originalFilename := "../data/sample.json"

	// Copy the original sample.json to test_sample_create.json
	input, err := os.ReadFile(originalFilename)
	if err != nil {
		t.Fatalf("Failed to read original sample.json: %s", err)
	}

	err = os.WriteFile(filename, input, 0644)
	if err != nil {
		t.Fatalf("Failed to create test JSON file: %s", err)
	}
	// REMOVE THIS LATER
	// defer os.Remove(filename)

	// Initialize a Handler instance
	handler := &Handler{
		Node: &types.Node{
			// Initialize the Node fields as necessary
		},
	}

	// Define a new table to be created
	newTable := &Table{
		TableName:         "new_table",
		PartitionKeyNames: []string{"New Key"},
		Partitions: []*Partition{
			{
				Metadata: &Metadata{
					PartitionKey:       2,
					PartitionKeyValues: []string{"2"},
				},
				Rows: []*Row{
					{
						CreatedAt: EpochTime(time.Unix(0, 1464175411324447)),
						UpdatedAt: EpochTime(time.Unix(0, 1464175411324447)),
						DeletedAt: EpochTime(time.Unix(0, -1)),
						Cells: []*Cell{
							{Name: "New Cell", Value: "New Value"},
						},
					},
				},
			},
		},
	}

	// Call HandleCreate method
	err = handler.HandleCreate(filename, newTable)
	if err != nil {
		t.Errorf("HandleCreate failed: %s", err)
	} else {
		t.Logf("HandleCreate succeeded")
	}

	// Verify the table was created
	localData, err := ReadJSON(filename)
	if err != nil {
		t.Fatalf("Failed to read JSON: %s", err)
	}

	if !CheckTableExists("new_table", localData) {
		t.Errorf("New table was not created")
	} else {
		t.Logf("New table was successfully created")
	}
}
