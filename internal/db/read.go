package db

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	pb "github.com/uimagine-admin/tunadb/api"
)

type Row struct {
	PageId    string `json:"pageID"`
	Element   string `json:"element"`
	Timestamp string `json:"timestamp"`
	Event     string `json:"event"`
	UpdatedAt string `json:"updated_at"`
	CreatedAt string `json:"created_at"`
}

func HandleRead(nodeId int, req *pb.ReadRequest) ([]Row, error) {
	filename := fmt.Sprintf("../data/%d.json", nodeId)
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	byteValue, err := ioutil.ReadAll(file)
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
