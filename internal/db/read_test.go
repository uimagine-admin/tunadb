package db

import (
	"fmt"
	"log"
	"testing"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/utils"
)

func TestHandleRead(t *testing.T) {
	nodeId := "test-read-token-ranges"
	relativePathSaveDir := fmt.Sprintf("internal/data/%s.json", nodeId)
	absolutePathSaveDir := utils.GetPath(relativePathSaveDir)

	req := &pb.ReadRequest{
		PageId: "page2",
	}

	rows, err := HandleRead(nodeId, req, absolutePathSaveDir)
	if err != nil {
		t.Fatalf("HandleRead failed: %s", err)
	}

	for _, row := range rows {
		log.Printf("Row: %+v\n", row)
	}
}

func TestFetchRecordsByHashKey(t *testing.T) {
	nodeId := "test-read-token-ranges"
	relativePathSaveDir := fmt.Sprintf("internal/data/%s.json", nodeId)
	absolutePathSaveDir := utils.GetPath(relativePathSaveDir)

	rowData, err :=  HandleRecordsFetchByHashKey("test-read-token-ranges", ring.TokenRange{
		Start: 0,
		End:   2,
	}, absolutePathSaveDir)

	if err != nil {
		t.Fatalf("HandleRecordsFetchByHashKey failed: %s", err)
	}


	records := []map[string]string{
		{
		  "page_id": "1",
		  "component_id": "button2",
		  "timestamp": "2023-11-26T12:01:00Z",
		  "event": "hover",
		  "updated_at": "",
		  "created_at": "",
		  "hashKey": "1",
		},
		{
		  "page_id": "2",
		  "component_id": "link1",
		  "timestamp": "2023-11-26T12:02:00Z",
		  "event": "click",
		  "updated_at": "",
		  "created_at": "",
		  "hashKey": "1",
		},
		{
		  "page_id": "3",
		  "component_id": "button1",
		  "timestamp": "2023-11-26T12:03:00Z",
		  "event": "hover",
		  "updated_at": "",
		  "created_at": "",
		  "hashKey": "1",
		},
		{
		  "page_id": "4",
		  "component_id": "button1",
		  "timestamp": "2023-11-26T12:04:00Z",
		  "event": "click",
		  "updated_at": "",
		  "created_at": "",
		  "hashKey": "1",
		},
		{
		  "page_id": "5",
		  "component_id": "button2",
		  "timestamp": "2023-11-26T12:05:00Z",
		  "event": "hover",
		  "updated_at": "",
		  "created_at": "",
		  "hashKey": "1",
		},
	}

	for _, record := range records {
		// Check if the row is in the expected records
		found := false
		for _, row := range rowData {
			if record["page_id"] == row.Data["page_id"] && record["component_id"] == row.Data["component_id"] && record["timestamp"] == row.Data["timestamp"] {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Row not found in records: %+v", record)
		}
	}
}

func TestFetchRecordsByHashKey2(t *testing.T) {
	nodeId := "test-read-token-ranges"
	relativePathSaveDir := fmt.Sprintf("internal/data/%s.json", nodeId)
	absolutePathSaveDir := utils.GetPath(relativePathSaveDir)

	rowData, err :=  HandleRecordsFetchByHashKey("test-read-token-ranges", ring.TokenRange{
		Start: 2,
		End:   100,
	},absolutePathSaveDir)

	if err != nil {
		t.Fatalf("HandleRecordsFetchByHashKey failed: %s", err)
	}


	records := []map[string]string{
		{
			"page_id": "5",
			"component_id": "button1",
			"timestamp": "2023-11-26T12:07:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "3",
			},
			{
			"page_id": "5",
			"component_id": "button1",
			"timestamp": "2023-11-26T12:08:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "4",
			},
			{
			"page_id": "5",
			"component_id": "button1",
			"timestamp": "2023-11-26T12:09:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "5",
			},
			{
			"page_id": "5",
			"component_id": "button1",
			"timestamp": "2023-11-26T12:10:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "6",
			},
			{
			"page_id": "5",
			"component_id": "button1",
			"timestamp": "2023-11-26T12:11:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "7",
			},
			{
			"page_id": "5",
			"component_id": "button1",
			"timestamp": "2023-11-26T12:12:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "8",
			},
			{
			"page_id": "5",
			"component_id": "button1",
			"timestamp": "2023-11-26T12:13:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "9",
			},
	}

	for _, record := range records {
		// Check if the row is in the expected records
		found := false
		for _, row := range rowData {
			if record["page_id"] == row.Data["page_id"] && record["component_id"] == row.Data["component_id"] && record["timestamp"] == row.Data["timestamp"] {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Record not found in rows: %+v", record)
		}
	}
}


func TestHandleReadAfterInsert(t *testing.T) {
	nodeId := "test-read-after-insert"
	relativePathSaveDir := fmt.Sprintf("internal/data/%s.json", nodeId)
	absolutePathSaveDir := utils.GetPath(relativePathSaveDir)

	req := &pb.WriteRequest{
		PageId:      "page2",
		ComponentId: "button1",
		Date:        "2023-11-26T12:14:00Z",
		Event:       "click",
		HashKey:     10,
	}

	err := HandleInsert(nodeId, req, absolutePathSaveDir)
	if err != nil {
		t.Fatalf("HandleInsert failed: %s", err)
	}

	readReq := &pb.ReadRequest{
		PageId:      "page2",
		Date:        "2023-11-26T12:14:00Z",
		Columns:  []string{"event", "componentId", "count"},
		Name:     nodeId,
		NodeType: "IS_NODE",
	}
	row, err := HandleRead(nodeId, readReq, absolutePathSaveDir)

	if err != nil {
		t.Fatalf("HandleRead failed: %s", err)
	}

	// check if the row is in the expected records
	found := false
	for _, r := range row {
		if r.PageId == req.PageId && r.ComponentId == req.ComponentId && r.Timestamp == req.Date {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Row not found in records: %+v", req)
	}
}

func TestReadAfterDuplicateInsert(t *testing.T) {
	nodeId := "test-read-after-insert-duplicates"
	relativePathSaveDir := fmt.Sprintf("internal/data/%s.json", nodeId)
	absolutePathSaveDir := utils.GetPath(relativePathSaveDir)

	req := &pb.WriteRequest{
		PageId:      "page2",
		ComponentId: "button1",
		Date:        "2023-11-26T12:15:00Z",
		Event:       "click",
		HashKey:     11,
	}

	err := HandleInsert(nodeId, req, absolutePathSaveDir)
	if err != nil {
		t.Fatalf("HandleInsert failed: %s", err)
	}

	err = HandleInsert(nodeId, req, absolutePathSaveDir)
	if err != nil {
		t.Fatalf("HandleInsert failed: %s", err)
	}

	readReq := &pb.ReadRequest{
		PageId:      "page2",
		Date:        "2023-11-26T12:15:00Z",
		Columns:  []string{"event", "componentId", "count"},
		Name:     nodeId,
		NodeType: "IS_NODE",
	}
	rows, err := HandleRead(nodeId, readReq, absolutePathSaveDir)

	if err != nil {
		t.Fatalf("HandleRead failed: %s", err)
	}

	// check if the row is in the expected records
	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}

	row := rows[0]
	if !(row.PageId == req.PageId && row.ComponentId == req.ComponentId && row.Timestamp == req.Date) {
		t.Fatalf("Row not found in records: %+v", req)
	}
}