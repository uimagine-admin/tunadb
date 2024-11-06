package db

import (
	"os"
	"testing"
	"time"
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

func TestGetTable(t *testing.T) {
	filename := "../data/sample.json" // change this to whatever suits u

	data, err := ReadJSON(filename)
	if err != nil {
		t.Fatalf("ReadJSON failed: %s", err)
	}

	table := GetTable("sample", data)
	if table == nil {
		t.Errorf("GetTable failed to retrieve existing table")
	} else {
		t.Logf("GetTable succeeded: %+v", table)
	}

	table = GetTable("nonexistent_table", data)
	if table != nil {
		t.Errorf("GetTable retrieved non-existent table")
	} else {
		t.Logf("GetTable correctly did not find non-existent table")
	}
}

func TestGetPartition(t *testing.T) {
	filename := "../data/sample.json" // change this to whatever suits u

	data, err := ReadJSON(filename)
	if err != nil {
		t.Fatalf("ReadJSON failed: %s", err)
	}

	table := GetTable("sample", data)
	if table == nil {
		t.Fatalf("GetTable failed to retrieve existing table")
	}

	partition := GetPartition(table, 1)
	if partition == nil {
		t.Errorf("GetPartition failed to retrieve existing partition")
	} else {
		t.Logf("GetPartition succeeded: %+v", partition)
	}

	partition = GetPartition(table, 999)
	if partition != nil {
		t.Errorf("GetPartition retrieved non-existent partition")
	} else {
		t.Logf("GetPartition correctly did not find non-existent partition")
	}
}

func TestPersistNewTable(t *testing.T) {
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
	// NOTE: IF YOU COMMENT ON THE LINE BELOW, YOU WILL BE ABLE TO INTRODUCE A NEW FILE IN THE OTHER DIRECTORY
	defer os.Remove(filename)

	// Read the original data
	data, err := ReadJSON(filename)
	if err != nil {
		t.Fatalf("ReadJSON failed: %s", err)
	}

	// Create a new table to persist
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

	// Test PersistNewTable function
	err = PersistNewTable(data, filename, newTable)
	if err != nil {
		t.Errorf("PersistNewTable failed: %s", err)
	}

	// Verify the table was persisted
	persistedData, err := ReadJSON(filename)
	if err != nil {
		t.Errorf("Failed to read persisted data: %s", err)
	}

	if len(persistedData) != 2 {
		t.Errorf("PersistNewTable did not persist the table correctly: %+v", persistedData)
	} else {
		t.Logf("PersistNewTable succeeded: %+v", persistedData)
	}
}
