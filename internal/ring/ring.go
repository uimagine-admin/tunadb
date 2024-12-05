package ring

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/spaolacci/murmur3"
	"github.com/uimagine-admin/tunadb/internal/types"
)

type TokenRange struct {
	Start uint64
	End   uint64
}

type ConsistentHashingRing struct {
	ring            map[uint64]types.Node
	sortedKeys      []uint64
	numVirtualNodes uint64
	numReplicas     int
	uniqueNodes     []types.Node
	mu    sync.RWMutex

	mapTokenRangesToNodeIDs map[string][]string
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

/*
	Public Method: Add Node
	This methods is to remove a node from the ring 

	Returns: The old token ranges for each node before the new node was added, this can be used by
	the distribution handler to redistribute the data only for nodes that have changed
*/
func (chr *ConsistentHashingRing) AddNode(node types.Node) map[string][]string {
	chr.mu.Lock()
	defer chr.mu.Unlock()

	copyOfMapTokenRangesToNodeIDs := make(map[string][]string)
	for tokenRangeString, nodeIDs := range chr.mapTokenRangesToNodeIDs {
		copyNodeIds := make([]string, len(nodeIDs))
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
func (chr *ConsistentHashingRing) DeleteNode(node types.Node) map[string][]string {
	chr.mu.Lock()
	defer chr.mu.Unlock()

	copyOfMapTokenRangesToNodeIDs := make(map[string][]string)
	for tokenRangeString, nodeIDs := range chr.mapTokenRangesToNodeIDs {
		copyNodeIds := make([]string, len(nodeIDs))
		copyOfMapTokenRangesToNodeIDs[tokenRangeString] = copyNodeIds
	}

	for i := 0; i < int(chr.numVirtualNodes); i++ {
		replicaKey := fmt.Sprintf("%s: %d", node.ID, i)
		hashKey := chr.calculateHash(replicaKey)
		delete(chr.ring, hashKey)
		chr.sortedKeys = removeFromSlice(chr.sortedKeys, hashKey)
	}
	chr.uniqueNodes = removeFromSlice(chr.uniqueNodes, node)

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

// Public Method: Get Nodes (returns multiple nodes for replication)
func (chr *ConsistentHashingRing) GetRecordsReplicas(key string) ( uint64, []types.Node){
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

	return hashKey, nodes
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

func (chr *ConsistentHashingRing) getNextKey(key uint64) uint64 {
	idx := sort.Search(len(chr.sortedKeys), func(i int) bool {
		return chr.sortedKeys[i] >= key
	})

	idx = (idx + 1) % len(chr.sortedKeys)
	return chr.sortedKeys[idx]
}

func (chr *ConsistentHashingRing) GetTokenRangeForNode(nodeID string) []TokenRange {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	tokenRanges := make([]TokenRange, 0)
	for tokenRangeStr, nodeIDs := range chr.mapTokenRangesToNodeIDs {
		for _, nID := range nodeIDs {
			if nID == nodeID {
				startEnd  := strings.Split(tokenRangeStr, ":")
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
		currentNode := chr.ring[currentKey]

		nextKey := chr.getNextKey(currentKey)

		tokenRange := fmt.Sprintf("%d:%d", currentKey, nextKey)

		mapTokenRangesToNodeID[tokenRange] = []string{currentNode.ID}

		// Assign replicas from the previous numReplicas keys
		for replica := 1; replica <= chr.numReplicas ; replica++ {
			replicaIndex := (i - replica + len(chr.sortedKeys)) % len(chr.sortedKeys)
			replicaKey := chr.sortedKeys[replicaIndex]
			replicaNode := chr.ring[replicaKey]

			// Append the replica node ID to the token range's list
			mapTokenRangesToNodeID[tokenRange] = append(mapTokenRangesToNodeID[tokenRange], replicaNode.ID)
		}
	}

	chr.mapTokenRangesToNodeIDs = mapTokenRangesToNodeID
}


func (chr *ConsistentHashingRing) GetRingMembers() []types.Node {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	return chr.uniqueNodes
}