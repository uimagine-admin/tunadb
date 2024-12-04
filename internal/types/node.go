package types

import (
	"fmt"
	"sync"
	"time"
)

// NodeStatus represents the state of a node in the cluster.
type NodeStatus string

const (
	NodeStatusAlive   NodeStatus = "ALIVE"
	NodeStatusSuspect NodeStatus = "SUSPECT"
	NodeStatusDead    NodeStatus = "DEAD"
)



// Node represents a single node in the distributed system.
type Node struct {
	IPAddress   string
	ID          string
	Port        uint64
	Name        string
	Status      NodeStatus    // Current status of the node
	LastUpdated time.Time     // Timestamp of the last update from this node
	mu          sync.RWMutex  // Protects access to node fields
}



//String provides a formatted string representation of the Node for debugging purposes.
func (n *Node) String() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return fmt.Sprintf("Node[ID=%s, Name=%s, IP=%s, Port=%d, Status=%s, LastUpdated=%s]",
		n.ID, n.Name, n.IPAddress, n.Port, n.Status, n.LastUpdated.Format(time.RFC3339))
}

// UpdateStatus updates the node's status and timestamp for the last update.
func (n *Node) UpdateStatus(status NodeStatus) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Status = status
	n.LastUpdated = time.Now()
}

// IsAlive checks if the node is considered alive based on its status.
func (n *Node) IsAlive() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Status == NodeStatusAlive
}

// Equals checks if two nodes are identical by comparing their unique IDs.
func (n *Node) Equals(other *Node) bool {
	return n.ID == other.ID
}


// Test Utility Functions
func (n *Node) IsDead() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Status == NodeStatusDead
}