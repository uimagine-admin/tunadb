package replication

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
)

// helper function to run the goroutines that simulate responses from replicas
func simulateReadResponses(responses []*pb.ReadResponse, resultsChan chan<- *pb.ReadResponse) {
	var wg sync.WaitGroup

	for _, resp := range responses {
		wg.Add(1)
		go func(resp *pb.ReadResponse) {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond) // Simulate network delay
			resultsChan <- resp
		}(resp)
	}

	wg.Wait()
	close(resultsChan)
}

// helper function to check if the quorum response is as expected
func runReceiveReadQuorumTest(t *testing.T, responses []*pb.ReadResponse, numReplicas int, expectError bool) {
	resultsChan := make(chan *pb.ReadResponse, numReplicas)
	go simulateReadResponses(responses, resultsChan)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quorumResponse, err,_ := ReceiveReadQuorum(ctx, resultsChan, numReplicas)
	if expectError {
		if err == nil {
			t.Fatalf("Expected error, but got response: %v", quorumResponse)
		}
		if quorumResponse != nil {
			t.Fatalf("Expected nil quorumResponse, but got: %v", quorumResponse)
		}
	} else {
		if err != nil {
			t.Fatalf("Expected quorum to be reached, but got error: %v", err)
		}
		if quorumResponse == nil {
			t.Fatalf("Expected quorumResponse to be non-nil")
		}
	}
}

// func TestReceiveQuorum(t *testing.T) {
// 	numReplicas := 3
// 	columns := []string{"Date", "PageId", "Event", "ComponentId"}
// 	nodeType := "IS_NODE"
// 	var wg sync.WaitGroup

// 	resultsChan := make(chan *pb.ReadResponse, numReplicas)

// 	// Simulate responses from replicas
// 	responses := []*pb.ReadResponse{
// 		{
// 			Name:     "Node1",
// 			Date:     "2024-03-12",
// 			PageId:   "1",
// 			Columns:  columns,
// 			NodeType: nodeType,
// 			Rows:     []*pb.RowData{{Data: map[string]string{"event": "click", "componentId": "button1"}}},
// 		},
// 		{
// 			Name:     "Node2",
// 			Date:     "2024-03-11",
// 			PageId:   "1",
// 			Columns:  columns,
// 			NodeType: nodeType,
// 			Rows:     []*pb.RowData{{Data: map[string]string{"event": "click", "componentId": "button1"}}},
// 		},
// 		// You can add more responses if needed
// 	}

// 	// Simulate the goroutines that send the responses
// 	for _, resp := range responses {
// 		wg.Add(1)
// 		go func(resp *pb.ReadResponse) {
// 			defer wg.Done()
// 			resultsChan <- resp
// 			time.Sleep(10 * time.Millisecond) // Simulate network delay
// 		}(resp)
// 	}

// 	go func() {
// 		wg.Wait()
// 		close(resultsChan) // close the channel here since we guarantee that all replicas have sent their responses
// 	}()

// 	// Use a context with a timeout
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	// Call ReceiveQuorum in the same way it's called in your code
// 	quorumResponse, err := ReceiveReadQuorum(ctx, resultsChan, numReplicas)
// 	if err != nil {
// 		t.Fatalf("Expected quorum to be reached, but got error: %v", err)
// 	}

// 	if quorumResponse == nil {
// 		t.Fatalf("Expected quorumResponse to be non-nil")
// 	}

// 	// Verify that the response has the most recent date
// 	expectedDate := "2024-03-12"
// 	if quorumResponse.Date != expectedDate {
// 		t.Fatalf("Expected date %s, but got %s", expectedDate, quorumResponse.Date)
// 	}

// 	t.Logf("Quorum reached with response from %s with date %s", quorumResponse.Name, quorumResponse.Date)
// }
// func TestReceiveQuorum_NotReached(t *testing.T) {
// 	numReplicas := 3
// 	resultsChan := make(chan *pb.ReadResponse, numReplicas)
// 	defer close(resultsChan)

// 	// Simulate fewer responses than needed for quorum
// 	responses := []*pb.ReadResponse{
// 		{
// 			Name:   "Node1",
// 			Date:   "2024-03-11",
// 			PageId: "1",
// 			Rows:   []*pb.RowData{{Data: map[string]string{"event": "click", "componentId": "button1"}}},
// 		},
// 	}

// 	go func() {
// 		for _, resp := range responses {
// 			resultsChan <- resp
// 			time.Sleep(10 * time.Millisecond)
// 		}
// 	}()

// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	quorumResponse, err := ReceiveReadQuorum(ctx, resultsChan, numReplicas)
// 	if err == nil {
// 		t.Fatalf("Expected quorum not to be reached, but got response: %v", quorumResponse)
// 	}

// 	if quorumResponse != nil {
// 		t.Fatalf("Expected quorumResponse to be nil when quorum is not reached")
// 	}

// 	t.Log("Quorum was not reached as expected")
// }

func TestReceiveReadQuorum_Success(t *testing.T) {
	numReplicas := 3
	columns := []string{"Date", "PageId", "Event", "ComponentId"}
	nodeType := "IS_NODE"

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
			Rows:     []*pb.RowData{{Data: map[string]string{"event": "scroll", "componentId": "section1"}}},
		},
		{
			Name:     "Node3",
			Date:     "2024-03-13",
			PageId:   "1",
			Columns:  columns,
			NodeType: nodeType,
			Rows:     []*pb.RowData{{Data: map[string]string{"event": "hover", "componentId": "menu1"}}},
		},
	}

	runReceiveReadQuorumTest(t, responses, numReplicas, false)
}

func TestReceiveReadQuorum_QuorumNotReached(t *testing.T) {
	numReplicas := 3

	responses := []*pb.ReadResponse{
		{
			Name:   "Node1",
			Date:   "2024-03-10",
			PageId: "1",
			Rows:   []*pb.RowData{{Data: map[string]string{"event": "click"}}},
		},
	}

	runReceiveReadQuorumTest(t, responses, numReplicas, true)
}

func TestReceiveReadQuorum_InvalidDates(t *testing.T) {
	numReplicas := 3

	responses := []*pb.ReadResponse{
		{
			Name:   "Node1",
			Date:   "invalid-date",
			PageId: "1",
			Rows:   []*pb.RowData{{Data: map[string]string{"event": "click"}}},
		},
		{
			Name:   "Node2",
			Date:   "another-invalid-date",
			PageId: "1",
			Rows:   []*pb.RowData{{Data: map[string]string{"event": "scroll"}}},
		},
		{
			Name:   "Node3",
			Date:   "2024-03-13",
			PageId: "1",
			Rows:   []*pb.RowData{{Data: map[string]string{"event": "hover"}}},
		},
	}

	runReceiveReadQuorumTest(t, responses, numReplicas, false)
}

func TestReceiveReadQuorum_Timeout(t *testing.T) {
	numReplicas := 3

	responses := []*pb.ReadResponse{
		{
			Name:   "Node1",
			Date:   "2024-03-12",
			PageId: "1",
			Rows:   []*pb.RowData{{Data: map[string]string{"event": "click"}}},
		},
	}

	// Set a very short timeout to simulate timeout scenario
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultsChan := make(chan *pb.ReadResponse, numReplicas)
	go simulateReadResponses(responses, resultsChan)

	quorumResponse, err,_ := ReceiveReadQuorum(ctx, resultsChan, numReplicas)
	if err == nil {
		t.Fatalf("Expected timeout error, but got response: %v", quorumResponse)
	}
}

func TestReceiveReadQuorum_ManyReplicas(t *testing.T) {
	numReplicas := 10
	columns := []string{"Date", "PageId", "Event", "ComponentId"}
	nodeType := "IS_NODE"

	var responses []*pb.ReadResponse
	for i := 0; i < numReplicas; i++ {
		date := "2024-03-12"
		if i%2 == 0 {
			date = "2024-03-14" // Some nodes have a more recent date
		}
		responses = append(responses, &pb.ReadResponse{
			Name:     fmt.Sprintf("Node%d", i+1),
			Date:     date,
			PageId:   "1",
			Columns:  columns,
			NodeType: nodeType,
			Rows:     []*pb.RowData{{Data: map[string]string{"event": "test"}}},
		})
	}

	runReceiveReadQuorumTest(t, responses, numReplicas, false)
}

func TestReceiveWriteQuorum(t *testing.T) {
	numReplicas := 3
	var wg sync.WaitGroup

	resultsChan := make(chan *pb.WriteResponse, numReplicas)

	// Simulate responses from replicas
	responses := []*pb.WriteResponse{
		{
			Ack:      true,
			Name:     "Node1",
			NodeType: "IS_NODE",
		},
		{
			Ack:      true,
			Name:     "Node2",
			NodeType: "IS_NODE",
		},
	}

	// Simulate the goroutines that send the responses
	for _, resp := range responses {
		wg.Add(1)
		go func(resp *pb.WriteResponse) {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond) // Simulate network delay
			resultsChan <- resp
		}(resp)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quorumResponse, err := ReceiveWriteQuorum(ctx, resultsChan, numReplicas)
	if err != nil {
		t.Fatalf("Expected quorum to be reached, but got error: %v", err)
	}

	if !quorumResponse.Ack {
		t.Fatalf("Expected Ack to be true")
	}

	t.Logf("Quorum reached with response from %s", quorumResponse.Name)
}
func TestReceiveWriteQuorum_ManyReplicas(t *testing.T) {
	numReplicas := 10
	var wg sync.WaitGroup

	resultsChan := make(chan *pb.WriteResponse, numReplicas)

	// Simulate the goroutines that send the responses
	// for ( i := 0; i < numReplicas; i++) {
	for i := 0; i < numReplicas; i++ {
		var resp *pb.WriteResponse
		if i%2 == 0 || i == 1 {
			resp = &pb.WriteResponse{
				Ack:      true,
				Name:     fmt.Sprintf("Node%d", i+1),
				NodeType: "IS_NODE",
			}
		} else {
			resp = &pb.WriteResponse{
				Ack:      false,
				Name:     fmt.Sprintf("Node%d", i+1),
				NodeType: "IS_NODE",
			}
		}
		wg.Add(1)
		go func(resp *pb.WriteResponse) {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond) // Simulate network delay
			resultsChan <- resp
		}(resp)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quorumResponse, err := ReceiveWriteQuorum(ctx, resultsChan, numReplicas)
	if err != nil {
		t.Fatalf("Expected quorum to be reached, but got error: %v", err)
	}

	if !quorumResponse.Ack {
		t.Fatalf("Expected Ack to be true")
	}

	t.Logf("Quorum reached with response from %s", quorumResponse.Name)
}

func TestReceiveWriteQuorum_NotReached(t *testing.T) {
	numReplicas := 3
	resultsChan := make(chan *pb.WriteResponse, numReplicas)
	defer close(resultsChan)

	// Simulate fewer responses than needed for quorum
	responses := []*pb.WriteResponse{
		{
			Ack:      true,
			Name:     "Node1",
			NodeType: "IS_NODE",
		},
	}

	go func() {
		for _, resp := range responses {
			resultsChan <- resp
			time.Sleep(10 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quorumResponse, err := ReceiveWriteQuorum(ctx, resultsChan, numReplicas)
	if err == nil {
		t.Fatalf("Expected quorum not to be reached, but got response: %v", quorumResponse)
	}

	if quorumResponse != nil {
		t.Fatalf("Expected quorumResponse to be nil when quorum is not reached")
	}

	t.Log("Quorum was not reached as expected")
}

func TestReceiveWriteQuorum_1FalseAckIn2Ack(t *testing.T) {
	numReplicas := 3
	resultsChan := make(chan *pb.WriteResponse, numReplicas)
	defer close(resultsChan)

	// Simulate fewer responses than needed for quorum
	responses := []*pb.WriteResponse{
		{
			Ack:      false,
			Name:     "Node1",
			NodeType: "IS_NODE",
		},
		{
			Ack:      true,
			Name:     "Node2",
			NodeType: "IS_NODE",
		},
	}

	go func() {
		for _, resp := range responses {
			resultsChan <- resp
			time.Sleep(10 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quorumResponse, err := ReceiveWriteQuorum(ctx, resultsChan, numReplicas)
	if err == nil {
		t.Fatalf("Expected quorum not to be reached, but got response: %v", quorumResponse)
	}

	if quorumResponse != nil {
		t.Fatalf("Expected quorumResponse to be nil when quorum is not reached")
	}

	t.Log("Quorum was not reached as expected")
}
func TestReceiveWriteQuorum_2FalseAckIn3Ack(t *testing.T) {
	numReplicas := 3
	resultsChan := make(chan *pb.WriteResponse, numReplicas)
	defer close(resultsChan)

	// Simulate fewer responses than needed for quorum
	responses := []*pb.WriteResponse{
		{
			Ack:      false,
			Name:     "Node1",
			NodeType: "IS_NODE",
		},
		{
			Ack:      false,
			Name:     "Node3",
			NodeType: "IS_NODE",
		},
		{
			Ack:      true,
			Name:     "Node2",
			NodeType: "IS_NODE",
		},
	}

	go func() {
		for _, resp := range responses {
			resultsChan <- resp
			time.Sleep(10 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quorumResponse, err := ReceiveWriteQuorum(ctx, resultsChan, numReplicas)
	if err == nil {
		t.Fatalf("Expected quorum not to be reached, but got response: %v", quorumResponse)
	}

	if quorumResponse != nil {
		t.Fatalf("Expected quorumResponse to be nil when quorum is not reached")
	}

	t.Log("Quorum was not reached as expected")
}
