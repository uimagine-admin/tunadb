package db

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
)

func HandleInsert(nodeId string, req *pb.WriteRequest) error {
	filename := fmt.Sprintf("../data/%s.json", nodeId)

	var row []Row

	// Check if the file exists
	if _, err := os.Stat(filename); err == nil {
		// File exists, read the existing data
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&row); err != nil {
			return fmt.Errorf("failed to decode JSON: %w", err)
		}
	} else if os.IsNotExist(err) {
		// File does not exist, create a new list
		row = []Row{}
	} else {
		return fmt.Errorf("failed to check file existence: %w", err)
	}

	// Create a new event object
	newEvent := Row{
		Event:       req.Event,
		Timestamp:   time.Now().Format(time.RFC3339),
		ComponentId: req.ComponentId,
		PageId:      req.PageId,
		UpdatedAt:   time.Now().Format(time.RFC3339),
		CreatedAt:   time.Now().Format(time.RFC3339),
	}

	// Append the new event to the list
	row = append(row, newEvent)

	// Write the updated data back to the file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(row); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return nil
}
