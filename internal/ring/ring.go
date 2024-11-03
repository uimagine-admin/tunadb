package ring

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spaolacci/murmur3"
	"github.com/uimagine-admin/tunadb/internal/types"
)

type ConsistentHashingRing struct {
	ring            map[uint64]types.Node
	sortedKeys      []uint64
	numVirtualNodes uint64
	numReplicas     int
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
	for i := 0; i < int(chr.numVirtualNodes); i++ {
		replicaKey := fmt.Sprintf("%s: %d", node.ID, i)
		hashed := chr.calculateHash(replicaKey)
		chr.ring[hashed] = node
		chr.sortedKeys = append(chr.sortedKeys, hashed)
	}
	sort.Slice(chr.sortedKeys, func(i, j int) bool {
		return chr.sortedKeys[i] < chr.sortedKeys[j]
	})
}

// Public Method: Remove Node
func (chr *ConsistentHashingRing) DeleteNode(node types.Node) {
	for i := 0; i < int(chr.numVirtualNodes); i++ {
		replicaKey := fmt.Sprintf("%s: %d", node.ID, i)
		hashKey := chr.calculateHash(replicaKey)
		delete(chr.ring, hashKey)
		chr.sortedKeys = removeFromSlice(chr.sortedKeys, hashKey)
	}
}

// util function
func removeFromSlice(slice []uint64, value uint64) []uint64 {
	for i, v := range slice {
		if value == v {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// Public Method: Get Nodes (returns multiple nodes for replication)
func (chr *ConsistentHashingRing) GetNodes(key string) []types.Node {
	if len(chr.ring) == 0 {
		return nil
	}

	hashKey := chr.calculateHash(key)

	idx := sort.Search(len(chr.sortedKeys), func(i int) bool {
		return chr.sortedKeys[i] >= hashKey
	})

	var nodes []types.Node
	for i := 0; i < chr.numReplicas; i++ {
		currentIdx := (idx + i) % len(chr.sortedKeys)
		hashAtIdx := chr.sortedKeys[currentIdx]
		node := chr.ring[hashAtIdx]
		nodes = append(nodes, node)
	}

	return nodes
}
