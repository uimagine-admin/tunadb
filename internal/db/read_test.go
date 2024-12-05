package db

import (
	"log"
	"testing"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/ring"
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

func TestFetchRecordsByHashKey(t *testing.T) {
	rowData, err :=  HandleRecordsFetchByHashKey("test_tokenRange", ring.TokenRange{
		Start: 0,
		End:   2,
	})

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
	rowData, err :=  HandleRecordsFetchByHashKey("test_tokenRange", ring.TokenRange{
		Start: 2,
		End:   100,
	})

	if err != nil {
		t.Fatalf("HandleRecordsFetchByHashKey failed: %s", err)
	}


	records := []map[string]string{
		{
			"page_id": "5",
			"component_id": "button3",
			"timestamp": "2023-11-26T12:06:00Z",
			"event": "hover",
			"updated_at": "",
			"created_at": "",
			"hashKey": "2",
			},
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


