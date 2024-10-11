package ring

import (
	"testing"

	"github.com/uimagine-admin/tunadb/internal/types"
)

// this file is to test the ring.go
// will be useful if we decided to implement more customaized approach beside the normal one

// START OF TESTS

// 1. testCreateConsistentHashingRing - this one is for checking if the ring gets created properly
func TestCreateConsistentHashingRing(t *testing.T) {
	t.Log("Creating a consistent hashing ring with 3 replicas.")
	ring := CreateConsistentHashingRing(3)
	if ring == nil {
		t.Fatal("Expected a non-nil ring") // we want a valid ring, not nil - something wrong in the arg or return prolly?
	}
	t.Log("Ring created successfully!")

	t.Logf("Checking if numReplicas is set correctly, expected 3, got %d.", ring.numReplicas)
	if ring.numReplicas != 3 {
		// oops, expected it to be 3 but it’s not!
		t.Errorf("Expected numReplicas to be 3, got %d", ring.numReplicas)
	}

	t.Log("Checking if the ring map is empty initially.")
	if len(ring.ring) != 0 {
		// It should start empty before we add any nodes
		t.Errorf("Expected empty ring map, got %d entries", len(ring.ring))
	} else {
		t.Log("Ring map is empty as expected.")
	}
}

// 2. TestAddNode - Here we add a node to see if it ends up in the ring properly
func TestAddNode(t *testing.T) {
	t.Log("Creating a consistent hashing ring with 3 replicas.")
	ring := CreateConsistentHashingRing(3)

	node := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
	t.Logf("Adding node: %+v", node)
	ring.AddNode(node)

	t.Log("Checking if the node has been added correctly with 3 replicas.")
	if len(ring.ring) != int(ring.numReplicas) {
		t.Errorf("Expected %d replicas in the ring, but got %d", ring.numReplicas, len(ring.ring))
	} else {
		t.Logf("Node added successfully with %d replicas.", ring.numReplicas)
	}
}

// 3.TestGetNode - let's check if we can get the right node for a given key
func TestGetNode(t *testing.T) {
	t.Log("Creating a consistent hashing ring with 3 replicas.")
	ring := CreateConsistentHashingRing(3)

	nodeA := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
	nodeB := types.Node{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081}
	t.Logf("Adding nodes: %+v and %+v", nodeA, nodeB)
	ring.AddNode(nodeA)
	ring.AddNode(nodeB)

	key := "user123"
	t.Logf("Getting node responsible for key '%s'", key)
	assignedNode := ring.GetNode(key)
	if assignedNode == nil {
		t.Fatal("Expected a node to be assigned, but got nil")
	}
	t.Logf("Assigned node for key '%s' is: %+v", key, assignedNode)

	// it should be either nodeA or nodeB, so lets check that
	if assignedNode.ID != "1" && assignedNode.ID != "2" {
		t.Errorf("Expected node ID to be '1' or '2', got %s", assignedNode.ID)
	} else {
		t.Logf("Key '%s' is correctly assigned to node '%s'.", key, assignedNode.ID)
	}
}

// 4. testDeleteNode - check if removing a node actually works
func TestDeleteNode(t *testing.T) {
	t.Log("Creating a consistent hashing ring with 3 replicas.")
	ring := CreateConsistentHashingRing(3)

	nodeA := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
	nodeB := types.Node{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081}
	t.Logf("Adding nodes: %+v and %+v", nodeA, nodeB)
	ring.AddNode(nodeA)
	ring.AddNode(nodeB)

	t.Logf("Checking initial number of replicas, expected %d, got %d.", int(ring.numReplicas)*2, len(ring.ring))
	if len(ring.ring) != int(ring.numReplicas)*2 {
		t.Fatalf("Expected %d replicas in the ring, got %d", int(ring.numReplicas)*2, len(ring.ring))
	}

	t.Log("Now deleting nodeA and checking the ring.")
	ring.DeleteNode(nodeA)
	if len(ring.ring) != int(ring.numReplicas) {
		// something went wrong if this fails
		t.Errorf("Expected %d replicas in the ring after deleting nodeA, but got %d", int(ring.numReplicas), len(ring.ring))
	} else {
		t.Logf("nodeA deleted successfully, remaining replicas: %d", len(ring.ring))
	}

	// here we try to double check that a query returns the other node now
	key := "user456"
	t.Logf("Querying for key '%s' after deletion of nodeA.", key)
	assignedNode := ring.GetNode(key)
	if assignedNode == nil {
		t.Fatal("Expected a node to be assigned, got nil") // can't be nil!
	}
	t.Logf("Assigned node for key '%s' is: %+v", key, assignedNode)
	if assignedNode.ID != "2" {
		// we should only have nodeB left after removing nodeA.
		t.Errorf("Expected node ID '2' after deletion, but got %s", assignedNode.ID)
	} else {
		t.Log("Deletion of nodeA successful, key is now assigned to nodeB.")
	}
}
