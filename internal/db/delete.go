package db

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	pb "github.com/uimagine-admin/tunadb/api"
)

var GeneralSuccess = "\033[38;5;40m"         // Green 

func HandleDelete(nodeId string, req *pb.DeleteRequest, absolutePathSaveDir string) error {
	var rows []Row

	// Check if the file exists
	if _, err := os.Stat(absolutePathSaveDir); err == nil {
		// File exists, read the existing data
		file, err := os.Open(absolutePathSaveDir)
		if err != nil {
			return fmt.Errorf(GeneralError + "[%s] Failed to open file: %w" + Reset, nodeId, err)
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			return fmt.Errorf(GeneralError + "[%s] Failed to read file: %w" + Reset, nodeId, err)
		}

		if len(data) > 0 {
			if err := json.Unmarshal(data, &rows); err != nil {
				return fmt.Errorf(GeneralError+ "[%s] Failed to unmarshal JSON: %w" + Reset, nodeId, err)
			}
		}
	} else if os.IsNotExist(err) {
		// File does not exist, nothing to delete
		return fmt.Errorf(GeneralError + "[%s] Data file does not exist" + Reset, nodeId)
	} else {
		return fmt.Errorf(GeneralError + "[%s] Failed to check file existence: %w" + Reset, nodeId, err)
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

	log.Printf(GeneralSuccess + "[%s] Deleted %d rows\n" + Reset, nodeId , len(rows)-len(updatedRows))

	// Write the updated data back to the file
	file, err := os.Create(absolutePathSaveDir)
	if err != nil {
		return fmt.Errorf(GeneralError + "[%s] Failed to create file: %w" + Reset, nodeId, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(updatedRows); err != nil {
		return fmt.Errorf(GeneralError + "[%s] Failed to encode JSON: %w" + Reset, nodeId, err)
	}

	return nil
}
