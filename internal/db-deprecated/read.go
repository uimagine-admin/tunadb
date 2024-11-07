package db

import "fmt"

func (h *Handler) HandleRead(filename string, tableName string, partitionKey int64) (*ReadResponse, error) {
	// ReadJSON
	localData, err := ReadJSON(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON: %w", err)
	}

	// GetTable
	table := GetTable(tableName, localData)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// GetPartition
	partition := GetPartition(table, partitionKey)
	if partition == nil {
		return nil, fmt.Errorf("partition with key %d not found", partitionKey)
	}

	return &ReadResponse{
		SourceNode: h.Node,
		Rows:       partition.Rows,
	}, nil
}
