package db

import (
	pb "github.com/uimagine-admin/tunadb/api"
)

func HandleInsert(nodeId string, req pb.WriteRequest) {
	// within nodeId.json --> create a new object under the list inside {Date: ..., Event: ..., Timestamp: ..., Element: ..., PageID: ..., updatedAt: ..., createdAt: ...}
	// return error
	
}
