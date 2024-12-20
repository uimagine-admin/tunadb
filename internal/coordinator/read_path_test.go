package coordinator

// import (
// 	"context"
// 	"fmt"
// 	"testing"
// 	"time"

// 	pb "github.com/uimagine-admin/tunadb/api"
// 	"github.com/uimagine-admin/tunadb/internal/replication"
// 	"github.com/uimagine-admin/tunadb/internal/ring"
// 	"github.com/uimagine-admin/tunadb/internal/types"
// )

// var mockResponses = map[string]*pb.ReadResponse{
// 	"node1": {Values: []string{"value1"}},
// 	"node2": {Values: []string{"value1"}},
// 	"node3": {Values: []string{"value2"}},
// }

// // Mock sendRead to return static responses based on the node name.
// // func sendRead(ctx context.Context, address string, req *pb.ReadRequest) (*pb.ReadResponse, error) {
// // 	if resp, ok := mockResponses[address]; ok {
// // 		return resp, nil
// // 	}
// // 	return nil, errors.New("node unavailable")
// // }

// func TestCoordinatorHandler_Read_QuorumReached(t *testing.T) {
// 	ring := ring.CreateConsistentHashingRing(3, 2)

// 	nodeA := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
// 	nodeB := types.Node{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081}
// 	nodeC := types.Node{ID: "3", Name: "NodeC", IPAddress: "192.168.1.3", Port: 8082}
// 	t.Logf("Adding nodes: %+v, %+v, and %+v", nodeA, nodeB, nodeC)
// 	ring.AddNode(nodeA)
// 	ring.AddNode(nodeB)
// 	ring.AddNode(nodeC)

// 	handler := NewCoordinatorHandler(
// 		ring,
// 		&nodeA,
// 	)

// 	// Test that Read correctly fetches the quorum value
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	val, err := handler.Read(ctx, "test_key")
// 	log.Println("Received value of", val)
// 	if err != nil {
// 		t.Fatalf("Error reading key: %v", err)
// 	}
// 	if val != "value1" {
// 		t.Fatalf("Expected quorum value to be 'value1', got %s", val)
// 	}
// }

// func TestCoordinatorHandler_Read_QuorumNotReached(t *testing.T) {
// 	ring := ring.CreateConsistentHashingRing(3, 2)

// 	nodeA := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
// 	nodeB := types.Node{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081}
// 	nodeC := types.Node{ID: "3", Name: "NodeC", IPAddress: "192.168.1.3", Port: 8082}
// 	t.Logf("Adding nodes: %+v, %+v, and %+v", nodeA, nodeB, nodeC)
// 	ring.AddNode(nodeA)
// 	ring.AddNode(nodeB)
// 	ring.AddNode(nodeC)

// 	handler := NewCoordinatorHandler(
// 		ring,
// 		&nodeA,
// 	)

// 	// Modify mock responses to make quorum unreachable
// 	mockResponses = map[string]*pb.ReadResponse{
// 		"NodeA": {Values: []string{"value1"}},
// 		"NodeB": {Values: []string{"value2"}},
// 		"NodeC": {Values: []string{"value3"}},
// 	}

// 	// Test that Read fails when quorum is not reached
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	val, err := handler.Read(ctx, "test_key")
// 	if err == nil && val != "" {
// 		t.Fatalf("Expected error due to lack of quorum, but got no error")
// 	}
// }
// func TestReceiveQuorum_Timeout(t *testing.T) {
// 	// Test quorum timeout by setting a short context timeout
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	resultsChan := make(chan *pb.ReadResponse)
// 	go func() {
// 		time.Sleep(10 * time.Millisecond) // Simulate delay in response
// 		resultsChan <- &pb.ReadResponse{Values: []string{"value1"}}
// 		close(resultsChan)
// 	}()

// 	val, err := replication.ReceiveQuorum(ctx, resultsChan, 3)
// 	if err.Error() != "read quorum not reached due to timeout or cancellation" {
// 		t.Fatalf("Expected timeout error, but got none")
// 	}
// 	if val != "" {
// 		t.Fatalf("Expected no quorum value due to timeout, but got %s", val)
// 	}
// }
// func TestReceiveQuorum_Success(t *testing.T) {
// 	ctx := context.Background()
// 	resultsChan := make(chan *pb.ReadResponse, 3)

// 	// Mock three responses, two of which match
// 	go func() {
// 		resultsChan <- &pb.ReadResponse{Values: []string{"value1"}}
// 		resultsChan <- &pb.ReadResponse{Values: []string{"value1"}}
// 		resultsChan <- &pb.ReadResponse{Values: []string{"value2"}}
// 		close(resultsChan)
// 	}()

// 	val, err := replication.ReceiveQuorum(ctx, resultsChan, 3)
// 	if err != nil {
// 		t.Fatalf("Expected quorum to be reached, but got error: %v", err)
// 	}
// 	if val != "value1" {
// 		t.Fatalf("Expected quorum value to be 'value1', got %s", val)
// 	}
// }
