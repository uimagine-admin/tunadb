package db

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/types"
)

func TestHandleInsert(t *testing.T) {
	for i := 0; i < 100; i++ {
		req := &pb.WriteRequest{
			PageId:      fmt.Sprintf("page%d", i),
			ComponentId: "button2",
			Event:       "hover",
			Date: time.Now().Format(time.RFC3339Nano),
		}
		err := HandleInsert("0", req)
		if err != nil {
			t.Errorf("HandleInsert failed for nodeId=0: %s", err)
		} else {
			log.Println("HandleInsert succeeded for nodeId=0")
		}
	}

	for i := 0; i < 100; i++ {
		req := &pb.WriteRequest{
			PageId:      fmt.Sprintf("page%d", i),
			ComponentId: "button3",
			Event:       "click",
			Date: time.Now().Format(time.RFC3339Nano),
		}
		err := HandleInsert("1", req)
		if err != nil {
			t.Errorf("HandleInsert failed for nodeId=1: %s", err)
		} else {
			log.Println("HandleInsert succeeded for nodeId=1")
		}
	}
}

func TestHandleInsert2(t *testing.T){
	nodeA := types.Node{ID: "3", Name: "NodeA", IPAddress: "127.0.0.1", Port: 50003}
	nodeAData := []map[string]string{
		{
			"pageID": "1",
			"element": "button2",
			"timestamp": "2023-11-26T12:01:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "1",
		  },
		  {
			"pageID": "2",
			"element": "link1",
			"timestamp": "2023-11-26T12:02:00Z",
			"event": "click",
			"updated_at": "",
			"created_at": "",
			"hashKey": "1",
		  },
		  {
			"pageID": "3",
			"element": "button1",
			"timestamp": "2023-11-26T12:03:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "1",
		  },
		  {
			"pageID": "4",
			"element": "button1",
			"timestamp": "2023-11-26T12:04:00Z",
			"event": "click",
			"updated_at": "",
			"created_at": "",
			"hashKey": "1",
		  },
		  {
			"pageID": "5",
			"element": "button2",
			"timestamp": "2023-11-26T12:05:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "1",
		  },
		  {
			"pageID": "5",
			"element": "button3",
			"timestamp": "2023-11-26T12:06:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "2",
		  },
		  {
			"pageID": "5",
			"element": "button1",
			"timestamp": "2023-11-26T12:07:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "3",
		  },
		  {
			"pageID": "5",
			"element": "button1",
			"timestamp": "2023-11-26T12:08:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "4",
		  },
		  {
			"pageID": "5",
			"element": "button1",
			"timestamp": "2023-11-26T12:09:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "5",
		  },
		  {
			"pageID": "5",
			"element": "button1",
			"timestamp": "2023-11-26T12:10:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "6",
		  },
		  {
			"pageID": "5",
			"element": "button1",
			"timestamp": "2023-11-26T12:11:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "7",
		  },
		  {
			"pageID": "5",
			"element": "button1",
			"timestamp": "2023-11-26T12:12:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "8",
		  },
		  {
			"pageID": "5",
			"element": "button1",
			"timestamp": "2023-11-26T12:13:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "9",
		  },
	}

	for _, row := range nodeAData {
		hashKey, errParse := strconv.ParseUint(row["hashKey"], 10, 64)
		if errParse != nil {
			t.Errorf("Failed to parse hash key: %v", errParse)
		}
		err := HandleInsert(nodeA.ID, &pb.WriteRequest{
			Date:         row["timestamp"],
			PageId:       row["pageID"],
			Event:        row["event"],
			ComponentId:  row["element"],
			HashKey:      hashKey,
			NodeType:     "IS_NODE",
			Name:         "NodeA",
		})
		if err != nil {
			t.Errorf("Failed to insert data: %v", err)
		}
	}
}
