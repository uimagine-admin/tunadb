package gossip

import (
	"log"
	"sync"
	"time"

	chr "github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
)

type Membership struct {
	currentNode *types.Node
	mu    sync.RWMutex
	nodes map[string]*types.Node
}

// NewMembership initializes a new Membership instance
func NewMembership(currentNodeInformation *types.Node) *Membership {

	// Initialize the membership with the current node
	m := &Membership{
		nodes: make(map[string]*types.Node),
		currentNode : currentNodeInformation,
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
func (m *Membership) AddOrUpdateNode(node *types.Node, chr *chr.ConsistentHashingRing) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existingNode, exists := m.nodes[node.Name]
	incomingNode := types.Node{
		ID:         node.ID,
		Name:       node.Name,
		IPAddress:  node.IPAddress,
		Port:       node.Port,
		Status:     node.Status,
		LastUpdated: node.LastUpdated,
	}

	ringUpdated := false


	// case 1: Incoming message has a more recent timestamp than the existing node
	if exists && incomingNode.LastUpdated.After(existingNode.LastUpdated) {
		//case 1.1 1.2: node is currently alive or suspect, and the incoming node is alive
		if incomingNode.Status == types.NodeStatusAlive && (existingNode.Status == types.NodeStatusAlive || existingNode.Status == types.NodeStatusSuspect) {
			m.nodes[incomingNode.Name] = &incomingNode
			log.Printf("Node[%s] Updating node: %v\n", m.currentNode.ID, incomingNode.String())
		}

		// case 1.3: node is currently marked As Dead but the incoming node is alive
		if incomingNode.Status == types.NodeStatusAlive && existingNode.Status == types.NodeStatusDead {
			m.nodes[incomingNode.Name] = &incomingNode
			ringUpdated = true
			log.Printf("Node[%s] Updating node: %v\n", m.currentNode.ID, incomingNode.String())
		}

		// case 1.4: incoming node is marked as suspect and the existing node is alive
		if incomingNode.Status == types.NodeStatusSuspect && existingNode.Status == types.NodeStatusAlive {
			m.nodes[incomingNode.Name] = &incomingNode
			log.Printf("Node[%s] Updating node: %v\n", m.currentNode.ID, incomingNode.String())
		}

		// case 1.5: incoming node is marked as suspect and the existing node is suspect
		// Just let current node time out and set Node to Dead 

		// case 1.6: incoming node is marked as suspect and the existing node is dead
		// This should not happen,but even if it does, we can ignore it

		// case 1.7: incoming node is marked as dead and the existing node is alive
		if incomingNode.Status == types.NodeStatusDead && existingNode.Status == types.NodeStatusAlive {
			m.nodes[incomingNode.Name] = &incomingNode
			ringUpdated = true
			log.Printf("Node[%s] Updating node: %v\n", m.currentNode.ID, incomingNode.String())
		}

		// case 1.8: incoming node is marked as dead and the existing node is suspect
		// Just let the current node time out and set Node to Dead

		// case 1.9: incoming node is marked as dead and the existing node is dead
		// TODO: consider if we need to remove the node from the membership list
	}

	// case 2: node is new 
	if !exists{
		m.nodes[incomingNode.Name] = &incomingNode
		ringUpdated = true
		log.Printf("Node[%s] Updating node: %v\n", m.currentNode.ID, incomingNode.String())
	}

	// if node has not been seen before, add it to the consistent hashing ring
	if ringUpdated == true {
		if !chr.DoesRingContainNode(&incomingNode) {
			log.Printf("Node[%s] Adding node: %s\n", m.currentNode.ID, incomingNode.String())
			chr.AddNode(incomingNode)
		} else if incomingNode.Status == types.NodeStatusDead {
			log.Printf("Node[%s] Deleting node: %s\n", m.currentNode.ID, incomingNode.String())
			chr.DeleteNode(incomingNode)
		}
	}
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
