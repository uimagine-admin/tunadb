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
	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ring := CreateConsistentHashingRing(3, 2)
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
	if len(ring.ring) != 0 {
		// It should start empty before we add any nodes
		t.Errorf("Expected empty ring map, got %d entries", len(ring.ring))
	} else {
		t.Log("Ring map is empty as expected.")
	}
}

// 2. TestAddNode - Here we add a node to see if it ends up in the ring properly
func TestAddNode(t *testing.T) {
	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ring := CreateConsistentHashingRing(3, 2)

	node := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
	t.Logf("Adding node: %+v", node)
	ring.AddNode(node)

	t.Log("Checking if the node has been added correctly with 3 virtual nodes.")
	if len(ring.ring) != int(ring.numVirtualNodes) {
		t.Errorf("Expected %d virtual nodes in the ring, but got %d", ring.numVirtualNodes, len(ring.ring))
	} else {
		t.Logf("Node added successfully with %d virtual nodes.", ring.numVirtualNodes)
	}

	t.Log("Verifying that the correct node is returned for a key.")
	key := "testKey"
	assignedNodes := ring.GetNodes(key)
	if len(assignedNodes) == 0 {
		t.Errorf("Expected at least 1 node to be assigned for the key, but got none.")
	} else {
		t.Logf("Assigned nodes for key '%s': %+v", key, assignedNodes)
	}
}

// 3. TestGetNodes - let's check if we can get the correct nodes for a given key
func TestGetNodes(t *testing.T) {
	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ring := CreateConsistentHashingRing(3, 2)

	nodeA := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
	nodeB := types.Node{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081}
	t.Logf("Adding nodes: %+v and %+v", nodeA, nodeB)
	ring.AddNode(nodeA)
	ring.AddNode(nodeB)

	key := "user123"
	t.Logf("Getting nodes responsible for key '%s'", key)
	assignedNodes := ring.GetNodes(key)
	if len(assignedNodes) != ring.numReplicas {
		t.Fatalf("Expected %d nodes to be assigned, but got %d", ring.numReplicas, len(assignedNodes))
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
	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ring := CreateConsistentHashingRing(3, 2)

	nodeA := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
	nodeB := types.Node{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081}
	t.Logf("Adding nodes: %+v and %+v", nodeA, nodeB)
	ring.AddNode(nodeA)
	ring.AddNode(nodeB)

	t.Logf("Checking initial number of virtual nodes, expected %d, got %d.", int(ring.numVirtualNodes)*2, len(ring.ring))
	if len(ring.ring) != int(ring.numVirtualNodes)*2 {
		t.Fatalf("Expected %d virtual nodes in the ring, got %d", int(ring.numVirtualNodes)*2, len(ring.ring))
	}

	t.Log("Now deleting nodeA and checking the ring.")
	ring.DeleteNode(nodeA)
	if len(ring.ring) != int(ring.numVirtualNodes) {
		t.Errorf("Expected %d virtual nodes in the ring after deleting nodeA, but got %d", int(ring.numVirtualNodes), len(ring.ring))
	} else {
		t.Logf("nodeA deleted successfully, remaining virtual nodes: %d", len(ring.ring))
	}

	// Double-check that a query returns the remaining node
	key := "user456"
	t.Logf("Querying for key '%s' after deletion of nodeA.", key)
	assignedNodes := ring.GetNodes(key)
	if len(assignedNodes) == 0 {
		t.Fatal("Expected nodes to be assigned, got none")
	}
	t.Logf("Assigned nodes for key '%s' are: %+v", key, assignedNodes)

	for _, assignedNode := range assignedNodes {
		if assignedNode.ID != "2" {
			t.Errorf("Expected node ID '2' after deletion, but got %s", assignedNode.ID)
		} else {
			t.Log("Deletion of nodeA successful, key is now assigned to nodeB.")
		}
	}
}

// 5. TestHashDistribution - test to ensure that the hash distribution is reasonably balanced
func TestHashDistribution(t *testing.T) {
	t.Log("Creating a consistent hashing ring with 10 virtual nodes and 3 replicas.")
	ring := CreateConsistentHashingRing(10, 3)

	nodes := []types.Node{
		{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080},
		{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081},
		{ID: "3", Name: "NodeC", IPAddress: "192.168.1.3", Port: 8082},
	}

	for _, node := range nodes {
		ring.AddNode(node)
	}

	hashCounts := make(map[string]int)
	keys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10"}

	for _, key := range keys {
		assignedNodes := ring.GetNodes(key)
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
	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ring := CreateConsistentHashingRing(3, 2)

	nodeA := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
	nodeB := types.Node{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081}
	ring.AddNode(nodeA)
	ring.AddNode(nodeB)

	t.Logf("Checking initial number of virtual nodes, expected %d, got %d.", int(ring.numVirtualNodes)*2, len(ring.ring))
	if len(ring.ring) != int(ring.numVirtualNodes)*2 {
		t.Fatalf("Expected %d virtual nodes in the ring, got %d", int(ring.numVirtualNodes)*2, len(ring.ring))
	}

	t.Log("Now deleting nodeA and checking the ring.")
	ring.DeleteNode(nodeA)
	if len(ring.ring) != int(ring.numVirtualNodes) {
		t.Errorf("Expected %d virtual nodes in the ring after deleting nodeA, but got %d", int(ring.numVirtualNodes), len(ring.ring))
	} else {
		t.Logf("nodeA deleted successfully, remaining virtual nodes: %d", len(ring.ring))
	}

	// Double-check that a query returns the remaining node
	key := "user456"
	t.Logf("Querying for key '%s' after deletion of nodeA.", key)
	assignedNodes := ring.GetNodes(key)
	if len(assignedNodes) == 0 {
		t.Fatal("Expected nodes to be assigned, got none")
	}
	t.Logf("Assigned nodes for key '%s' are: %+v", key, assignedNodes)

	for _, assignedNode := range assignedNodes {
		if assignedNode.ID != "2" {
			t.Errorf("Expected node ID '2' after deletion, but got %s", assignedNode.ID)
		} else {
			t.Log("Deletion of nodeA successful, key is now assigned to nodeB.")
		}
	}
}

func TestTokenRangeAddNode(t *testing.T) {
	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ring := CreateConsistentHashingRing(3, 2)

	nodeA := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
	nodeB := types.Node{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081}
	nodeC := types.Node{ID: "3", Name: "NodeC", IPAddress: "192.168.1.3", Port: 8082}
	ring.AddNode(nodeA)
	ring.AddNode(nodeB)
	ring.AddNode(nodeC)

	assert.True(t, len(ring.GetTokenRangeForNode("1")) == 3)
	assert.True(t, len(ring.GetTokenRangeForNode("2")) == 3)
	assert.True(t, len(ring.GetTokenRangeForNode("3")) == 3)

	// assert true that nodeA contain range [{9223291661934313288 11479556236612999045} {4300940255391410034 8598592649790674949} {16697655493772239060 17439072704036419864}]
	assert.True(t, ring.GetTokenRangeForNode("1")[0].Start == 9223291661934313288)
	assert.True(t, ring.GetTokenRangeForNode("1")[0].End == 11479556236612999045)
	assert.True(t, ring.GetTokenRangeForNode("1")[1].Start == 4300940255391410034)
	assert.True(t, ring.GetTokenRangeForNode("1")[1].End == 8598592649790674949)
	assert.True(t, ring.GetTokenRangeForNode("1")[2].Start == 16697655493772239060)
	assert.True(t, ring.GetTokenRangeForNode("1")[2].End == 17439072704036419864)

	// assert true that nodeB contain range 2:[{8598592649790674949 9223291661934313288} {12623532567914357714 14070063418868391783} {14070063418868391783 16697655493772239060}]
	assert.True(t, ring.GetTokenRangeForNode("2")[0].Start == 8598592649790674949)
	assert.True(t, ring.GetTokenRangeForNode("2")[0].End == 9223291661934313288)
	assert.True(t, ring.GetTokenRangeForNode("2")[1].Start == 12623532567914357714)
	assert.True(t, ring.GetTokenRangeForNode("2")[1].End == 14070063418868391783)
	assert.True(t, ring.GetTokenRangeForNode("2")[2].Start == 14070063418868391783)
	assert.True(t, ring.GetTokenRangeForNode("2")[2].End == 16697655493772239060)

	// assert true that nodeC contain range [{2919689892315055810 4300940255391410034} {17439072704036419864 2919689892315055810} {11479556236612999045 12623532567914357714}]]
	assert.True(t, ring.GetTokenRangeForNode("3")[0].Start == 2919689892315055810)
	assert.True(t, ring.GetTokenRangeForNode("3")[0].End == 4300940255391410034)
	assert.True(t, ring.GetTokenRangeForNode("3")[1].Start == 17439072704036419864)
	assert.True(t, ring.GetTokenRangeForNode("3")[1].End == 2919689892315055810)
	assert.True(t, ring.GetTokenRangeForNode("3")[2].Start == 11479556236612999045)
	assert.True(t, ring.GetTokenRangeForNode("3")[2].End == 12623532567914357714)
}

func TestTokenRangeDeleteNode(t *testing.T) {
	t.Log("Creating a consistent hashing ring with 3 virtual nodes and 2 replicas.")
	ring := CreateConsistentHashingRing(3, 2)

	nodeA := types.Node{ID: "1", Name: "NodeA", IPAddress: "192.168.1.1", Port: 8080}
	nodeB := types.Node{ID: "2", Name: "NodeB", IPAddress: "192.168.1.2", Port: 8081}
	nodeC := types.Node{ID: "3", Name: "NodeC", IPAddress: "192.168.1.3", Port: 8082}
	ring.AddNode(nodeA)
	ring.AddNode(nodeB)
	ring.AddNode(nodeC)

	ring.DeleteNode(nodeA)

	// assert true that nodeB contain range 2:[[{8598592649790674949 11479556236612999045} {12623532567914357714 14070063418868391783} {14070063418868391783 17439072704036419864}]
	assert.True(t, ring.GetTokenRangeForNode("2")[0].Start == 8598592649790674949)
	assert.True(t, ring.GetTokenRangeForNode("2")[0].End == 11479556236612999045)
	assert.True(t, ring.GetTokenRangeForNode("2")[1].Start == 12623532567914357714)
	assert.True(t, ring.GetTokenRangeForNode("2")[1].End == 14070063418868391783)
	assert.True(t, ring.GetTokenRangeForNode("2")[2].Start == 14070063418868391783)
	assert.True(t, ring.GetTokenRangeForNode("2")[2].End == 17439072704036419864)

	// assert true that nodeC contain range [{2919689892315055810 8598592649790674949} {17439072704036419864 2919689892315055810} {11479556236612999045 12623532567914357714}]
	assert.True(t, ring.GetTokenRangeForNode("3")[0].Start == 2919689892315055810)
	assert.True(t, ring.GetTokenRangeForNode("3")[0].End == 8598592649790674949)
	assert.True(t, ring.GetTokenRangeForNode("3")[1].Start == 17439072704036419864)
	assert.True(t, ring.GetTokenRangeForNode("3")[1].End == 2919689892315055810)
	assert.True(t, ring.GetTokenRangeForNode("3")[2].Start == 11479556236612999045)
	assert.True(t, ring.GetTokenRangeForNode("3")[2].End == 12623532567914357714)
}






