package ring

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// this file is to test the ring.go
// will be useful if we decided to implement more customized approach beside the normal one

// START OF TESTS

// 1. testCreateConsistentHashingRing - this one is for checking if the ring gets created properly
func TestCreateConsistentHashingRing(t *testing.T) {
	t.Log("Create current node")
	currentNode := &types.Node{ID: "1", Name: "NodeA", IPAddress: "127.0.0.1", Port: 8080}

	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ring := CreateConsistentHashingRing(currentNode,3, 2)
	if ring == nil {
		t.Fatal("Expected a non-nil ring") // we want a valid ring, not nil - something wrong in the arg or return probably?
	}
	t.Log("Ring created successfully!")

	t.Logf("Checking if numVirtualNodes is set correctly, expected 3, got %d.", ring.numVirtualNodes)
	if ring.numVirtualNodes != 3 {
		// oops, expected it to be 3 but it’s not!
		t.Errorf("Expected numVirtualNodes to be 3, got %d", ring.numVirtualNodes)
	}

	t.Logf("Checking if numReplicas is set correctly, expected 2, got %d.", ring.numReplicas)
	if ring.numReplicas != 2 {
		t.Errorf("Expected numReplicas to be 2, got %d", ring.numReplicas)
	}

	t.Log("Checking if the ring map is empty initially.")
	if len(ring.ring) != 3 {
		// It should start empty before we add any nodes
		t.Errorf("Ring to have 3 virtual nodes as per virtual node count, got %d entries", len(ring.ring))
	} else {
		t.Log("Ring map correctly initialized with 3 virtual nodes.")
	}
}

// 2. TestAddNode - Here we add a node to see if it ends up in the ring properly
func TestAddNode(t *testing.T) {
	t.Log("Create current node")
	currentNode := &types.Node{ID: "1", Name: "NodeA", IPAddress: "127.0.0.1", Port: 8080}

	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ring := CreateConsistentHashingRing(currentNode, 3, 2)

	nodeB := &types.Node{ID: "2", Name: "NodeB", IPAddress: "127.0.0.1", Port: 8081}
	t.Logf("Adding node: %+v", nodeB)
	ring.AddNode(nodeB)

	t.Log("Checking if the node has been added correctly with 6 virtual nodes.")
	if len(ring.ring) != 2 * int(ring.numVirtualNodes) {
		t.Errorf("Expected %d virtual nodes in the ring, but got %d", ring.numVirtualNodes, len(ring.ring))
	} else {
		t.Logf("Node added successfully with %d virtual nodes.", ring.numVirtualNodes)
	}

	t.Log("Verifying that the correct node is returned for a key.")
	key := "testKey"
	_, assignedNodes := ring.GetRecordsReplicas(key)
	if len(assignedNodes) == 0 {
		t.Errorf("Expected at least 1 node to be assigned for the key, but got none.")
	} else {
		t.Logf("Assigned nodes for key '%s': %+v", key, assignedNodes)
	}
}

// 3. TestGetRecordsReplicas - let's check if we can get the correct nodes for a given key
func TestGetRecordsReplicas(t *testing.T) {
	t.Log("Create current node")
	nodeA := &types.Node{ID: "1", Name: "NodeA", IPAddress: "127.0.0.1", Port: 8080}

	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ringViewA := CreateConsistentHashingRing(nodeA, 3, 2)

	nodeB := &types.Node{ID: "2", Name: "NodeB", IPAddress: "127.0.0.1", Port: 8081}

	t.Logf("Adding nodes: %+v and %+v", nodeA, nodeB)
	ringViewA.AddNode(nodeB)

	key := "user123"
	t.Logf("Getting nodes responsible for key '%s'", key)
	_, assignedNodes := ringViewA.GetRecordsReplicas(key)
	if len(assignedNodes) != ringViewA.numReplicas {
		t.Fatalf("Expected %d nodes to be assigned, but got %d", ringViewA.numReplicas, len(assignedNodes))
	}
	t.Logf("Assigned nodes for key '%s' are: %+v", key, assignedNodes)

	// Check that the returned nodes are within the set of added nodes
	for _, assignedNode := range assignedNodes {
		if assignedNode.ID != "1" && assignedNode.ID != "2" {
			t.Errorf("Expected node ID to be '1' or '2', got %s", assignedNode.ID)
		} else {
			t.Logf("Key '%s' is correctly assigned to node '%s'.", key, assignedNode.ID)
		}
	}
}

// 4. testDeleteNode - check if removing a node actually works
func TestDeleteNode(t *testing.T) {
	t.Log("Create current node")
	nodeA := &types.Node{ID: "1", Name: "NodeA", IPAddress: "127.0.0.1", Port: 8080}

	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ringViewA := CreateConsistentHashingRing(nodeA, 3, 2)

	nodeB := &types.Node{ID: "2", Name: "NodeB", IPAddress: "127.0.0.1", Port: 8081}

	t.Logf("Adding nodes: %+v", nodeB)
	ringViewA.AddNode(nodeB)

	t.Logf("Checking initial number of virtual nodes, expected %d, got %d.", int(ringViewA.numVirtualNodes)*2, len(ringViewA.ring))
	if len(ringViewA.ring) != int(ringViewA.numVirtualNodes)*2 {
		t.Fatalf("Expected %d virtual nodes in the ring, got %d", int(ringViewA.numVirtualNodes)*2, len(ringViewA.ring))
	}

	t.Log("Now deleting nodeB and checking the ring.")
	ringViewA.DeleteNode(nodeB)

	if len(ringViewA.ring) != int(ringViewA.numVirtualNodes) {
		t.Errorf("Expected %d virtual nodes in the ring after deleting nodeA, but got %d", int(ringViewA.numVirtualNodes), len(ringViewA.ring))
	} else {
		t.Logf("nodeA deleted successfully, remaining virtual nodes: %d", len(ringViewA.ring))
	}

	// Double-check that a query returns the remaining node
	key := "user456"
	t.Logf("Querying for key '%s' after deletion of nodeA.", key)
	_, assignedNodes := ringViewA.GetRecordsReplicas(key)
	if len(assignedNodes) == 0 {
		t.Fatal("Expected nodes to be assigned, got none")
	}
	t.Logf("Assigned nodes for key '%s' are: %+v", key, assignedNodes)

	for _, assignedNode := range assignedNodes {
		if assignedNode.ID != "1" {
			t.Errorf("Expected node ID '1' after deletion, but got %s", assignedNode.ID)
		} else {
			t.Log("Deletion of nodeB successful, key is now assigned to nodeB.")
		}
	}
}

// 5. TestHashDistribution - test to ensure that the hash distribution is reasonably balanced
func TestHashDistribution(t *testing.T) {
	t.Log("Create current node")
	nodeA := &types.Node{ID: "1", Name: "NodeA", IPAddress: "127.0.0.1", Port: 8080}

	t.Log("Creating a consistent hashing ring with 10 virtual nodes and 3 replicas.")
	ring := CreateConsistentHashingRing(nodeA, 10, 3)

	nodes := []*types.Node{
		{ID: "2", Name: "NodeB", IPAddress: "127.0.0.1", Port: 8081},
		{ID: "3", Name: "NodeC", IPAddress: "127.0.0.1", Port: 8082},
	}

	for _, node := range nodes {
		ring.AddNode(node)
	}

	hashCounts := make(map[string]int)
	keys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10"}

	for _, key := range keys {
		_, assignedNodes := ring.GetRecordsReplicas(key)
		for _, node := range assignedNodes {
			hashCounts[node.ID]++
		}
	}

	t.Log("Checking distribution of keys across nodes.")
	for nodeID, count := range hashCounts {
		t.Logf("Node ID '%s' has been assigned %d keys.", nodeID, count)
	}
}

// 6. TestDeleteNode - check if removing a node actually works
func TestDeleteNode2(t *testing.T) {
	t.Log("Create current node")
	nodeA := &types.Node{ID: "1", Name: "NodeA", IPAddress: "127.0.0.1", Port: 8080}

	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ringViewA := CreateConsistentHashingRing(nodeA, 3, 2)

	nodeB := &types.Node{ID: "2", Name: "NodeB", IPAddress: "127.0.0.1", Port: 8081}
	ringViewB := CreateConsistentHashingRing(nodeB, 3, 2)

	t.Logf("Adding nodes: %+v and %+v", nodeA, nodeB)
	ringViewA.AddNode(nodeB)
	ringViewB.AddNode(nodeA)

	t.Log("For ringViewA:")
	t.Logf("Checking initial number of virtual nodes, expected %d, got %d.", int(ringViewA.numVirtualNodes)*2, len(ringViewA.ring))
	if len(ringViewA.ring) != int(ringViewA.numVirtualNodes)*2 {
		t.Fatalf("Expected %d virtual nodes in the ring, got %d", int(ringViewA.numVirtualNodes)*2, len(ringViewA.ring))
	}

	t.Log("For ringViewB:")
	t.Logf("Checking initial number of virtual nodes, expected %d, got %d.", int(ringViewB.numVirtualNodes)*2, len(ringViewB.ring))
	if len(ringViewB.ring) != int(ringViewB.numVirtualNodes)*2 {
		t.Fatalf("Expected %d virtual nodes in the ring, got %d", int(ringViewB.numVirtualNodes)*2, len(ringViewB.ring))
	}

	t.Log("Now deleting nodeB and checking the ring A.")
	ringViewA.DeleteNode(nodeB)
	if len(ringViewA.ring) != int(ringViewA.numVirtualNodes) {
		t.Errorf("Expected %d virtual nodes in the ring after deleting nodeA, but got %d", int(ringViewA.numVirtualNodes), len(ringViewA.ring))
	} else {
		t.Logf("nodeA deleted successfully, remaining virtual nodes: %d", len(ringViewA.ring))
	}

	t.Log("Now deleting nodeA and checking the ring B.")
	ringViewB.DeleteNode(nodeA)
	if len(ringViewB.ring) != int(ringViewB.numVirtualNodes) {
		t.Errorf("Expected %d virtual nodes in the ring after deleting nodeA, but got %d", int(ringViewB.numVirtualNodes), len(ringViewB.ring))
	} else {
		t.Logf("nodeA deleted successfully, remaining virtual nodes: %d", len(ringViewB.ring))
	}

	// Double-check that a query returns the remaining node
	key := "user456"
	t.Logf("Querying for key '%s' after deletion of nodeB from ring view A.", key)
	_, assignedNodesRingA := ringViewA.GetRecordsReplicas(key)
	if len(assignedNodesRingA) == 0 {
		t.Fatal("Expected nodes to be assigned, got none")
	}
	t.Logf("Assigned nodes for key '%s' are: %+v", key, assignedNodesRingA)

	for _, assignedNode := range assignedNodesRingA {
		if assignedNode.ID != "1" {
			t.Errorf("Expected node ID '1' after deletion, but got %s", assignedNode.ID)
		} else {
			t.Log("Deletion of nodeB successful, key is now assigned to nodeA.")
		}
	}

	// Double-check that a query returns the remaining node
	t.Logf("Querying for key '%s' after deletion of nodeA from ring view B", key)
	_, assignedNodesRingB := ringViewB.GetRecordsReplicas(key)
	if len(assignedNodesRingB) == 0 {
		t.Fatal("Expected nodes to be assigned, got none")
	}
	t.Logf("Assigned nodes for key '%s' are: %+v", key, assignedNodesRingB)

	for _, assignedNode := range assignedNodesRingB {
		if assignedNode.ID != "2" {
			t.Errorf("Expected node ID '2' after deletion, but got %s", assignedNode.ID)
		} else {
			t.Log("Deletion of nodeA successful, key is now assigned to nodeB.")
		}
	}
}

func TestTokenRangeAddNode(t *testing.T) {
	t.Log("Create current node")
	nodeA := &types.Node{ID: "1", Name: "NodeA", IPAddress: "127.0.0.1", Port: 8080}

	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ringViewA := CreateConsistentHashingRing(nodeA, 3, 2)

	nodeB := &types.Node{ID: "2", Name: "NodeB", IPAddress: "127.0.0.1", Port: 8081}
	nodeC := &types.Node{ID: "3", Name: "NodeC", IPAddress: "127.0.0.1", Port: 8082}
	ringViewA.AddNode(nodeB)
	ringViewA.AddNode(nodeC)

	assert.True(t, len(ringViewA.GetTokenRangeForNode("1")) == 8)

	// check that node "1" has the {17439072704036419864 2919689892315055810}{16697655493772239060 17439072704036419864} {8598592649790674949 9223291661934313288}{12623532567914357714 14070063418868391783}{14070063418868391783 16697655493772239060} {2919689892315055810 4300940255391410034} {4300940255391410034 8598592649790674949} {11479556236612999045 12623532567914357714}
	token1 := TokenRange{Start: 17439072704036419864, End: 2919689892315055810}
	token2 := TokenRange{Start: 16697655493772239060, End: 17439072704036419864}
	token3 := TokenRange{Start: 8598592649790674949, End: 9223291661934313288}
	token4 := TokenRange{Start: 12623532567914357714, End: 14070063418868391783}
	token5 := TokenRange{Start: 14070063418868391783, End: 16697655493772239060}
	token6 := TokenRange{Start: 2919689892315055810, End: 4300940255391410034}
	token7 := TokenRange{Start: 4300940255391410034, End: 8598592649790674949}
	token8 := TokenRange{Start: 11479556236612999045, End: 12623532567914357714}

	tokenNeeded := []TokenRange{token1, token2, token3, token4, token5, token6, token7, token8}

	for _, token := range tokenNeeded {
		found := false
		for _, tokensIn := range ringViewA.GetTokenRangeForNode("1"){
			if token == tokensIn {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected token range %v to be present in node 1", token)
		}
	}
}







