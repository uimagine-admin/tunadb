package ring

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/spaolacci/murmur3"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// New struct to represent ring info in a JSON-friendly format
type RingInfo struct {
	Nodes       []NodeInfo          `json:"nodes"`
	TokenRanges map[string][]string `json:"token_ranges"`
}

// NodeInfo represents a node in a JSON-friendly manner
type NodeInfo struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	IPAddress string `json:"ip_address"`
	Port      uint64 `json:"port"`
	Status    string `json:"status"`
}

type TokenRange struct {
	Start uint64 // exclusive does not include the start
	End   uint64 // inclusive
}

type ConsistentHashingRing struct {
	ring                    map[uint64]*types.Node
	sortedKeys              []uint64
	numVirtualNodes         uint64
	numReplicas             int
	uniqueNodes             []*types.Node
	mu                      sync.RWMutex
	mapTokenRangesToNodeIDs map[string][]string
}

/*
Public function (constructor) - to be called outside in the main driver function

Take note that this methods takes in the currentNode ie the node/server that is
running on the current machine. The currentNode object passes to this node should
be the same object that is passed to the gossipHandler and the dataDistributionHandler
*/
func CreateConsistentHashingRing(currentNode *types.Node, numVirtualNodes uint64, numReplicas int) *ConsistentHashingRing {
	newRing := &ConsistentHashingRing{
		ring:                    make(map[uint64]*types.Node),
		sortedKeys:              []uint64{},
		numVirtualNodes:         numVirtualNodes,
		numReplicas:             numReplicas,
		mapTokenRangesToNodeIDs: make(map[string][]string),
	}
	newRing.AddNode(currentNode)
	return newRing
}

// Return ring info in a structured format for JSON output
func (chr *ConsistentHashingRing) GetRingInfo() *RingInfo {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	var nodesInfo []NodeInfo
	for _, n := range chr.uniqueNodes {
		nodesInfo = append(nodesInfo, NodeInfo{
			ID:        n.ID,
			Name:      n.Name,
			IPAddress: n.IPAddress,
			Port:      n.Port,
			Status:    string(n.Status),
		})
	}

	// Copy token ranges to avoid modification during iteration
	tokenRangesCopy := make(map[string][]string)
	for k, v := range chr.mapTokenRangesToNodeIDs {
		tokenRangesCopy[k] = v
	}

	return &RingInfo{
		Nodes:       nodesInfo,
		TokenRanges: tokenRangesCopy,
	}
}

func (chr *ConsistentHashingRing) GetTokenRangesInfo() *RingInfo {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	// Copy token ranges to avoid modification during iteration
	tokenRangesCopy := make(map[string][]string)
	for k, v := range chr.mapTokenRangesToNodeIDs {
		tokenRangesCopy[k] = v
	}

	return &RingInfo{
		TokenRanges: tokenRangesCopy,
	}
}

func (ring *ConsistentHashingRing) String() string {
	ring.mu.RLock()
	defer ring.mu.RUnlock()

	var ringDetails []string
	for key, node := range ring.ring {
		ringDetails = append(ringDetails, fmt.Sprintf("Key: %v, Node: %v", key, node))
	}

	return fmt.Sprintf(
		"ConsistentHashingRing:\n"+
			"NumVirtualNodes: %v\n"+
			"NumReplicas: %v\n"+
			"Ring:\n%v\n"+
			"SortedKeys: %v",
		ring.numVirtualNodes,
		ring.numReplicas,
		strings.Join(ringDetails, "\n"),
		ring.sortedKeys,
	)
}

func (chr *ConsistentHashingRing) calculateHash(key string) uint64 {
	hasher := murmur3.New64()
	hasher.Write([]byte(key))
	return hasher.Sum64()
}

/*
Public Method: Add Node
This methods is to add a node to the ring

!Only the gossip and the ring package can call this function.
Any other updates to the ring must be done via the membership method.

Returns: The old token ranges for each node before the new node was added, this can be used by
the distribution handler to redistribute the data only for nodes that have changed
*/
func (chr *ConsistentHashingRing) AddNode(node *types.Node) map[string][]string {
	chr.mu.Lock()
	defer chr.mu.Unlock()

	copyOfMapTokenRangesToNodeIDs := make(map[string][]string)
	for tokenRangeString, nodeIDs := range chr.mapTokenRangesToNodeIDs {
		copyNodeIds := make([]string, len(nodeIDs))
		copy(copyNodeIds, nodeIDs)
		copyOfMapTokenRangesToNodeIDs[tokenRangeString] = copyNodeIds
	}

	for i := 0; i < int(chr.numVirtualNodes); i++ {
		replicaKey := fmt.Sprintf("%s: %d", node.ID, i)
		hashed := chr.calculateHash(replicaKey)
		chr.ring[hashed] = node
		chr.sortedKeys = append(chr.sortedKeys, hashed)
	}
	sort.Slice(chr.sortedKeys, func(i, j int) bool {
		return chr.sortedKeys[i] < chr.sortedKeys[j]
	})
	chr.uniqueNodes = append(chr.uniqueNodes, node)

	chr.updateTokenRangesToNodeID()

	return copyOfMapTokenRangesToNodeIDs
}

/*
Public Method: Remove Node
This methods is to remove a node from the ring

Returns: The old token ranges for each node before the node was removed, this can be used by
the distribution handler to redistribute the data only for nodes that have changed
*/
func (chr *ConsistentHashingRing) DeleteNode(node *types.Node) map[string][]string {
	chr.mu.Lock()
	defer chr.mu.Unlock()

	copyOfMapTokenRangesToNodeIDs := make(map[string][]string)
	for tokenRangeString, nodeIDs := range chr.mapTokenRangesToNodeIDs {
		copyNodeIds := make([]string, len(nodeIDs))
		copy(copyNodeIds, nodeIDs)
		copyOfMapTokenRangesToNodeIDs[tokenRangeString] = copyNodeIds
	}

	for i := 0; i < int(chr.numVirtualNodes); i++ {
		replicaKey := fmt.Sprintf("%s: %d", node.ID, i)
		hashKey := chr.calculateHash(replicaKey)
		delete(chr.ring, hashKey)
		chr.sortedKeys = removeFromSlice(chr.sortedKeys, hashKey)
	}
	chr.uniqueNodes = removeNodeFromSlice(chr.uniqueNodes, node)

	chr.updateTokenRangesToNodeID()

	return copyOfMapTokenRangesToNodeIDs
}

// removeFromSlice removes an element from a slice and returns the new slice.
func removeFromSlice[T comparable](slice []T, value T) []T {
	for i, v := range slice {
		if value == v {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func removeNodeFromSlice(slice []*types.Node, node *types.Node) []*types.Node {
	for i, v := range slice {
		if v.ID == node.ID {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func (chr *ConsistentHashingRing) GetRecordsReplicas(key string) (uint64, []*types.Node) {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	if len(chr.ring) == 0 {
		return 0, nil
	}

	hashKey := chr.calculateHash(key)

	idx := sort.Search(len(chr.sortedKeys), func(i int) bool {
		return chr.sortedKeys[i] >= hashKey
	})

	numNodes := len(chr.uniqueNodes)
	if numNodes < chr.numReplicas {
		log.Println("WARN: Number of nodes in the ring is less than the number of replicas")
	}

	numOfNodesToReturn := min(numNodes, chr.numReplicas)

	var nodes []*types.Node
	i := 0
	for len(nodes) < numOfNodesToReturn {
		currentIdx := (idx + i) % len(chr.sortedKeys)
		hashAtIdx := chr.sortedKeys[currentIdx]
		node := chr.ring[hashAtIdx]

		// check for duplicates, so a virtual node is not returned
		if !containsNode(nodes, node) {
			nodes = append(nodes, node)
		}
		i++
	}

	return hashKey, nodes
}

// Utility function to check that the ring does not contain a node
// to be used only in Ring functions (hence private), methods calling
// this function should have a lock on the ring
// Uses the node ID to check for equality
func containsNode(nodes []*types.Node, node *types.Node) bool {
	for _, n := range nodes {
		if n.ID == node.ID {
			return true
		}
	}
	return false
}

// Public methods for other packages to check if the ring contains a node
// This function should not be called inside a method that already has a lock on the ring
func (chr *ConsistentHashingRing) DoesRingContainNode(node *types.Node) bool {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	for _, n := range chr.uniqueNodes {
		if n.ID == node.ID {
			return true
		}
	}
	return false
}

func (chr *ConsistentHashingRing) GetTokenRangeForNode(nodeID string) []TokenRange {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	tokenRanges := make([]TokenRange, 0)
	for tokenRangeStr, nodeIDs := range chr.mapTokenRangesToNodeIDs {
		for _, nID := range nodeIDs {
			if nID == nodeID {
				startEnd := strings.Split(tokenRangeStr, ":")
				rangeStart, errStr := strconv.ParseUint(startEnd[0], 10, 64)
				rangeEnd, errEnd := strconv.ParseUint(startEnd[1], 10, 64)

				if errStr != nil || errEnd != nil {
					log.Println("Error parsing token range")
					continue
				}

				tokenRanges = append(tokenRanges, TokenRange{Start: rangeStart, End: rangeEnd})
			}
		}
	}
	return tokenRanges
}

// util function to update the mapNodeToTokenRange, this data structure is used to store the token ranges for each node
// essential for data redistribution during node addition and removal
func (chr *ConsistentHashingRing) updateTokenRangesToNodeID() {
	mapTokenRangesToNodeID := make(map[string][]string)

	for i := 0; i < len(chr.sortedKeys); i++ {
		currentKey := chr.sortedKeys[i]
		nextKey := chr.sortedKeys[(i+1)%len(chr.sortedKeys)]

		tokenRange := fmt.Sprintf("%d:%d", currentKey, nextKey)

		numOfNodesToReturn := min(len(chr.uniqueNodes), chr.numReplicas)

		var nodes []*types.Node
		j := 1
		for len(nodes) < numOfNodesToReturn {
			currentIdx := (i + j) % len(chr.sortedKeys)
			hashAtIdx := chr.sortedKeys[currentIdx]
			node := chr.ring[hashAtIdx]

			// check for duplicates, so a virtual node is not returned
			if !containsNode(nodes, node) {
				nodes = append(nodes, node)
			}
			j++
		}

		var nodeIDs []string
		for _, node := range nodes {
			nodeIDs = append(nodeIDs, node.ID)
		}

		mapTokenRangesToNodeID[tokenRange] = nodeIDs
	}

	chr.mapTokenRangesToNodeIDs = mapTokenRangesToNodeID
}

func (chr *ConsistentHashingRing) GetRingMembers() []*types.Node {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	return chr.uniqueNodes
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Optional: JSON-encoding helper (not strictly required since GetRingInfo returns a ready-to-marshal struct)
func (chr *ConsistentHashingRing) MarshalJSON() ([]byte, error) {
	return json.Marshal(chr.GetRingInfo())
}
