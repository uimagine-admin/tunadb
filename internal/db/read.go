package db

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/ring"
)

func HandleRead(nodeId string, req *pb.ReadRequest) ([]Row, error) {
	filename := fmt.Sprintf("./internal/data/%s.json", nodeId)
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var rows []Row
	err = json.Unmarshal(byteValue, &rows)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	var result []Row
	for _, event := range rows {
		if event.PageId == req.PageId {
			result = append(result, event)
		}
	}

	return result, nil
}

func HandleRecordsFetchByHashKey(nodeId string, partitionKey ring.TokenRange) ([]*pb.RowData, error) {
	filename := fmt.Sprintf("./internal/data/%s.json", nodeId)
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var rows []Row
	err = json.Unmarshal(byteValue, &rows)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	var result []*pb.RowData
	for _, event := range rows {
		// parse event hashkey from string to uint64
		hashKey, err := strconv.ParseUint(event.HashKey, 10, 64)

		if err != nil {
			return nil, fmt.Errorf("failed to parse hashkey: %w", err)
		}

		if hashKey > partitionKey.Start && hashKey <= partitionKey.End {
			data := map[string]string{ 
				"page_id": event.PageId, 
				"component_id": event.ComponentId, 
				"timestamp": event.Timestamp, 
				"event": event.Event,
				"hashKey": event.HashKey,}
			result = append(result, &pb.RowData{Data: data})
		}
	}

	return result, nil
}
