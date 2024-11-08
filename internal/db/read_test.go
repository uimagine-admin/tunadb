package db

import (
	"log"
	"testing"

	pb "github.com/uimagine-admin/tunadb/api"
)

func TestHandleRead(t *testing.T) {
	nodeId := "1"
	log.Println("nodeId ", nodeId)

	req := &pb.ReadRequest{
		PageId: "page2",
	}

	rows, err := HandleRead(nodeId, req)
	if err != nil {
		t.Fatalf("HandleRead failed: %s", err)
	}

	for _, row := range rows {
		log.Printf("Row: %+v\n", row)
	}
}
