package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
)

func HandleInsert(nodeId string, req *pb.WriteRequest) error {
	basePath := os.Getenv("DATA_PATH")
	if basePath == "" {
		basePath = "./internal/db/internal/data/"
	}
	filename := filepath.Join(basePath, fmt.Sprintf("%s.json", nodeId))

	var rows []Row

	// Check if the file exists
	if _, err := os.Stat(filename); err == nil {
		// File exists, read the existing data
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("[%s] Failed to open file: %w", nodeId, err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&rows); err != nil {
			return fmt.Errorf("[%s] Failed to decode JSON: %w", nodeId, err)
		}
	} else if os.IsNotExist(err) {
		// File does not exist, create a new list
		rows = []Row{}
	} else {
		return fmt.Errorf("[%s] Failed to check file existence: %w",nodeId ,err)
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
			fmt.Printf("[%s] Record already exists: %+v\n", nodeId, newEvent)
			return nil
		}
	}

	// Append the new event to the list
	rows = append(rows, newEvent)

	// Write the updated data back to the file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("[%s] Failed to create file: %w", nodeId, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(rows); err != nil {
		return fmt.Errorf("[%s] Failed to encode JSON: %w", nodeId, err)
	}

	return nil
}
