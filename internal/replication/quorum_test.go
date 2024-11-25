package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
)

func TestReceiveQuorum(t *testing.T) {
	numReplicas := 3
	columns := []string{"Date", "PageId", "Event", "ComponentId"}
	nodeType := "IS_NODE"
	var wg sync.WaitGroup

	resultsChan := make(chan *pb.ReadResponse, numReplicas)

	// Simulate responses from replicas
	responses := []*pb.ReadResponse{
		{
			Name:     "Node1",
			Date:     "2024-03-12",
			PageId:   "1",
			Columns:  columns,
			NodeType: nodeType,
			Rows:     []*pb.RowData{{Data: map[string]string{"event": "click", "componentId": "button1"}}},
		},
		{
			Name:     "Node2",
			Date:     "2024-03-11",
			PageId:   "1",
			Columns:  columns,
			NodeType: nodeType,
			Rows:     []*pb.RowData{{Data: map[string]string{"event": "click", "componentId": "button1"}}},
		},
		// You can add more responses if needed
	}

	// Simulate the goroutines that send the responses
	for _, resp := range responses {
		wg.Add(1)
		go func(resp *pb.ReadResponse) {
			defer wg.Done()
			resultsChan <- resp
			time.Sleep(10 * time.Millisecond) // Simulate network delay
		}(resp)
	}

	go func() {
		wg.Wait()
		close(resultsChan) // close the channel here since we guarantee that all replicas have sent their responses
	}()

	// Use a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Call ReceiveQuorum in the same way it's called in your code
	quorumResponse, err := ReceiveQuorum(ctx, resultsChan, numReplicas)
	if err != nil {
		t.Fatalf("Expected quorum to be reached, but got error: %v", err)
	}

	if quorumResponse == nil {
		t.Fatalf("Expected quorumResponse to be non-nil")
	}

	// Verify that the response has the most recent date
	expectedDate := "2024-03-12"
	if quorumResponse.Date != expectedDate {
		t.Fatalf("Expected date %s, but got %s", expectedDate, quorumResponse.Date)
	}

	t.Logf("Quorum reached with response from %s with date %s", quorumResponse.Name, quorumResponse.Date)
}
func TestReceiveQuorum_NotReached(t *testing.T) {
	numReplicas := 3
	resultsChan := make(chan *pb.ReadResponse, numReplicas)
	defer close(resultsChan)

	// Simulate fewer responses than needed for quorum
	responses := []*pb.ReadResponse{
		{
			Name:   "Node1",
			Date:   "2024-03-11",
			PageId: "1",
			Rows:   []*pb.RowData{{Data: map[string]string{"event": "click", "componentId": "button1"}}},
		},
	}

	go func() {
		for _, resp := range responses {
			resultsChan <- resp
			time.Sleep(10 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	quorumResponse, err := ReceiveQuorum(ctx, resultsChan, numReplicas)
	if err == nil {
		t.Fatalf("Expected quorum not to be reached, but got response: %v", quorumResponse)
	}

	if quorumResponse != nil {
		t.Fatalf("Expected quorumResponse to be nil when quorum is not reached")
	}

	t.Log("Quorum was not reached as expected")
}
