package gossip

import (
	"sync"
	"time"

	"github.com/uimagine-admin/tunadb/internal/types"
)

type Membership struct {
	mu    sync.RWMutex
	nodes map[string]*types.Node
}

// NewMembership initializes a new Membership instance
func NewMembership(currentNodeInformation *types.Node) *Membership {

	// Initialize the membership with the current node
	m := &Membership{
		nodes: make(map[string]*types.Node),
	}
	
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodes[currentNodeInformation.Name] = &types.Node{
		ID:         currentNodeInformation.ID,
		Name:       currentNodeInformation.Name,
		IPAddress:  currentNodeInformation.IPAddress,
		Port:       currentNodeInformation.Port,
		Status:     currentNodeInformation.Status,
		LastUpdated: time.Now(),
	}

	return m;
}

// AddOrUpdateNode adds a new node or updates an existing node's information
func (m *Membership) AddOrUpdateNode(node *types.Node) *types.Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	existingNode, exists := m.nodes[node.Name]
	incomingNode := &types.Node{
		ID:         node.ID,
		Name:       node.Name,
		IPAddress:  node.IPAddress,
		Port:       node.Port,
		Status:     node.Status,
		LastUpdated: node.LastUpdated,
	}

	// case 1: node is already in the membership, just updating the status and the incoming node information is newer
	// node's time stamp should only be updated when the incoming node is alive
	// This should resolve any node that is marked as suspect or dead
	if exists && node.LastUpdated.After(existingNode.LastUpdated) && node.Status == types.NodeStatusAlive {
		m.nodes[node.Name] = incomingNode
		return m.nodes[node.Name]
	}

	// case 2: node is currently suspect or dead, and the incoming node is still dead or suspect
	// do not update the node's time stamp, we want to keep a record of the time it was marked as suspect or dead

	// case 3: node is new 
	if !exists{
		m.nodes[node.Name] = incomingNode
		return m.nodes[node.Name]
	}

	return nil
}

// MarkNodeSuspect marks a node as suspect if it hasn't responded within a threshold
func (m *Membership) MarkNodeSuspect(nodeName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeName]
	if exists && node.Status == types.NodeStatusAlive {
		node.Status = types.NodeStatusSuspect
		node.LastUpdated = time.Now()
	}
}

// MarkNodeDead marks a node as dead after prolonged unresponsiveness
func (m *Membership) MarkNodeDead(nodeName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeName]
	if exists {
		node.Status = types.NodeStatusDead
		node.LastUpdated = time.Now()
	}
}

// GetNode retrieves information about a specific node
func (m *Membership) GetNode(nodeName string) (*types.Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, exists := m.nodes[nodeName]
	return node, exists
}

// GetAllNodes retrieves information about all nodes in the cluster
func (m *Membership) GetAllNodes() map[string]*types.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()


	// Return a copy to avoid external modification
	nodesCopy := make(map[string]*types.Node)
	for name, info := range m.nodes {
		nodesCopy[name] = &types.Node{
			ID: 	   info.ID,
			Name:       info.Name,
			IPAddress:  info.IPAddress,
			Port:       info.Port,
			Status:     info.Status,
			LastUpdated: info.LastUpdated,
		}
	}
	return nodesCopy
}

// PruneDeadNodes removes nodes that have been dead for a certain duration
func (m *Membership) PruneDeadNodes(threshold time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, node := range m.nodes {
		if node.Status == types.NodeStatusDead && time.Since(node.LastUpdated) > threshold {
			delete(m.nodes, name)
		}
	}
}

// Heartbeat updates the last update time for a node and marks it alive
func (m *Membership) Heartbeat(nodeName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeName]
	if exists && node.Status != types.NodeStatusDead {
		node.Status = types.NodeStatusAlive
		node.LastUpdated = time.Now()
	}
}
