package db

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	pb "github.com/uimagine-admin/tunadb/api"
)

func HandleDelete(nodeId string, req *pb.DeleteRequest) error {
	filename := fmt.Sprintf("./internal/data/%s.json", nodeId)

	var rows []Row

	// Check if the file exists
	if _, err := os.Stat(filename); err == nil {
		// File exists, read the existing data
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		if len(data) > 0 {
			if err := json.Unmarshal(data, &rows); err != nil {
				return fmt.Errorf("failed to unmarshal JSON: %w", err)
			}
		}
	} else if os.IsNotExist(err) {
		// File does not exist, nothing to delete
		return fmt.Errorf("data file does not exist")
	} else {
		return fmt.Errorf("failed to check file existence: %w", err)
	}

	// Filter out the rows that match the delete criteria
	var updatedRows []Row
	for _, row := range rows {
		match := true

		if req.PageId != "" && row.PageId != req.PageId {
			match = false
		}
		if req.ComponentId != "" && row.ComponentId != req.ComponentId {
			match = false
		}
		if req.Event != "" && row.Event != req.Event {
			match = false
		}
		if req.Date != "" && row.Timestamp[:10] != req.Date { // Assuming date is in YYYY-MM-DD format
			match = false
		}

		if !match {
			updatedRows = append(updatedRows, row)
		}
	}

	log.Printf("Deleted %d rows\n", len(rows)-len(updatedRows))

	// Write the updated data back to the file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(updatedRows); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return nil
}
