package db

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
)

var Reset = "\033[0m"
var GeneralError = "\033[38;5;196m"          // Bright Red
var GeneralInfo = "\033[38;5;33m"            // Blue

func HandleInsert(nodeId string, req *pb.WriteRequest, absolutePathSaveDir string) error {
	var rows []Row
	// Check if the file exists
	if _, err := os.Stat(absolutePathSaveDir); err == nil {
		// File exists, read the existing data
		file, err := os.Open(absolutePathSaveDir)
		if err != nil {
			return fmt.Errorf(GeneralError + "[%s] Failed to open file: %w" + Reset, nodeId, err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&rows); err != nil {
			return fmt.Errorf(GeneralError + "[%s] Failed to decode JSON: %w" + Reset, nodeId, err)
		}
	} else if os.IsNotExist(err) {
		// File does not exist, create a new list
		rows = []Row{}
	} else {
		return fmt.Errorf(GeneralError + "[%s] Failed to check file existence: %w" + Reset, nodeId ,err)
	}

	// Create a new event object
	hashKey := strconv.FormatUint(req.HashKey, 10)
	newEvent := Row{
		PageId:      req.PageId,
		ComponentId: req.ComponentId,
		Timestamp:   req.Date,
		Event:       req.Event,
		UpdatedAt:   time.Now().Format(time.RFC3339Nano),
		CreatedAt:   time.Now().Format(time.RFC3339Nano),
		HashKey:     hashKey,
	}

	// Check if the record already exists
	for _, row := range rows {
		if row.PageId == newEvent.PageId &&
			row.ComponentId == newEvent.ComponentId &&
			row.Timestamp == newEvent.Timestamp &&
			row.Event == newEvent.Event {
			// Record already exists, skip insertion
			fmt.Printf(GeneralInfo + "[%s] Record already exists: %+v\n" + Reset, nodeId, newEvent)
			return nil
		}
	}

	// Append the new event to the list
	rows = append(rows, newEvent)

	// Write the updated data back to the file
	file, err := os.Create(absolutePathSaveDir)
	if err != nil {
		return fmt.Errorf(GeneralError + "[%s] Failed to create file: %w" + Reset, nodeId, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(rows); err != nil {
		return fmt.Errorf(GeneralError + "[%s] Failed to encode JSON: %w" + Reset, nodeId, err)
	}

	return nil
}
