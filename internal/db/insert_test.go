package db

import (
	"log"
	"testing"

	pb "github.com/uimagine-admin/tunadb/api"
)

func TestHandleInsert(t *testing.T) {
	req0 := &pb.WriteRequest{
		PageId:      "page1",
		ComponentId: "button2",
		Event:       "hover",
	}

	// Call HandleInsert for nodeId=0
	err := HandleInsert("0", req0)
	if err != nil {
		t.Errorf("HandleInsert failed for nodeId=0: %s", err)
	} else {
		log.Println("HandleInsert succeeded for nodeId=0")
	}

	// Define the WriteRequest for nodeId=1
	req1 := &pb.WriteRequest{
		PageId:      "page2",
		ComponentId: "button3",
		Event:       "click",
	}

	// Call HandleInsert for nodeId=1
	err = HandleInsert("1", req1)
	if err != nil {
		t.Errorf("HandleInsert failed for nodeId=1: %s", err)
	} else {
		log.Println("HandleInsert succeeded for nodeId=1")
	}

}
