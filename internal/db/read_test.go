package db

import (
	"fmt"
	"testing"

	pb "github.com/uimagine-admin/tunadb/api"
)

func TestHandleRead(t *testing.T) {
	nodeId := "0"
	fmt.Println("test ", nodeId)

	req := &pb.ReadRequest{
		PageId: "page1",
	}

	rows, err := HandleRead(nodeId, req)
	if err != nil {
		t.Fatalf("HandleRead failed: %s", err)
	}

	for _, row := range rows {
		fmt.Printf("Row: %+v\n", row)
	}
}
