package ring

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/spaolacci/murmur3"
	"github.com/uimagine-admin/tunadb/internal/types"
)

type ConsistentHashingRing struct {
	ring            map[uint64]types.Node
	sortedKeys      []uint64
	numVirtualNodes uint64
	numReplicas     int
	uniqueNodes     []types.Node
	mu    sync.RWMutex
}

// public function (constructor) - to be called outside in the main driver function
func CreateConsistentHashingRing(numVirtualNodes uint64, numReplicas int) *ConsistentHashingRing {
	return &ConsistentHashingRing{
		ring:            make(map[uint64]types.Node),
		sortedKeys:      []uint64{},
		numVirtualNodes: numVirtualNodes,
		numReplicas:     numReplicas,
	}
}

// Public and special method: util function to print consistent hashing ring prettily
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

// Private method: to calculate hash
func (chr *ConsistentHashingRing) calculateHash(key string) uint64 {
	hasher := murmur3.New64()
	hasher.Write([]byte(key))
	return hasher.Sum64()
}

// Public Method: to add node
func (chr *ConsistentHashingRing) AddNode(node types.Node) {
	chr.mu.Lock()
	defer chr.mu.Unlock()

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
}

// Public Method: Remove Node
func (chr *ConsistentHashingRing) DeleteNode(node types.Node) {
	chr.mu.Lock()
	defer chr.mu.Unlock()

	for i := 0; i < int(chr.numVirtualNodes); i++ {
		replicaKey := fmt.Sprintf("%s: %d", node.ID, i)
		hashKey := chr.calculateHash(replicaKey)
		delete(chr.ring, hashKey)
		chr.sortedKeys = removeFromSlice(chr.sortedKeys, hashKey)
	}
	chr.uniqueNodes = removeFromSlice(chr.uniqueNodes, node)
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

// Public Method: Get Nodes (returns multiple nodes for replication)
func (chr *ConsistentHashingRing) GetNodes(key string) []types.Node {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	if len(chr.ring) == 0 {
		return nil
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

	var nodes []types.Node
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

	return nodes
}

// util function to check if a node is already in the list
func containsNode(nodes []types.Node, node types.Node) bool {
	for _, n := range nodes {
		if n.ID == node.ID {
			return true
		}
	}
	return false
}

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
