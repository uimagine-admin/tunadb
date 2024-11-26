package gossip_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/gossip"
	rp "github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
	"google.golang.org/grpc"
)

type server struct {
	GossipHandler *gossip.GossipHandler
	pb.UnimplementedCassandraServiceServer
}

func (s *server) Gossip(ctx context.Context, req *pb.GossipMessage) (*pb.GossipAck, error) {
	// Call the gossip handler's method to process the message
	return s.GossipHandler.HandleGossipMessage(ctx, req)
}

// Helper to start a gRPC server for a gossip handler
func StartNode(handler *gossip.GossipHandler) (*grpc.Server, error) {
	grpcServer := grpc.NewServer()
	pb.RegisterCassandraServiceServer(grpcServer, &server{
		GossipHandler: handler,
	})
	

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", handler.NodeInfo.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Printf("Error serving gRPC for node %s: %v", handler.NodeInfo.Name, err)
		}
	}()

	return grpcServer, nil
}

/*
 Helper function to create an initial system with numNodes nodes and return the nodes and their corresponding rings
 and begin gossip protocol for each node

 numNodes: Number of nodes in the system
 gossipFanOut: Number of nodes to fan out each message
 replicationFactor: Number of replicas for each node
 numberOfVirtualNodes: Number of virtual nodes for each node
*/
func createInitialSystem(numNodes int, numberOfVirtualNodes uint64, replicationFactor int, gossipFanOut int, suspectToDeadTimeout int, gossipInterval int) ([]*types.Node,[]*gossip.GossipHandler, []*grpc.Server, []*rp.ConsistentHashingRing, []*context.Context, []*context.CancelFunc) {
	// Step 1: Initialize the cluster with a consistent hashing ring
	nodeRings := []*rp.ConsistentHashingRing{}
	for i := 0; i < numNodes; i++ {
		ring := rp.CreateConsistentHashingRing(numberOfVirtualNodes, replicationFactor)
		nodeRings = append(nodeRings, ring)
	}


	nodes := make([]*types.Node, numNodes)
	for i := 0; i < numNodes; i++ {
		node := &types.Node{
			IPAddress: "localhost",
			ID:     fmt.Sprintf("Node_%d", i),
			Port:   uint64(9000 + i),
			Name:   fmt.Sprintf("localhost:%d", 9000+i),
			Status: types.NodeStatusAlive,
			LastUpdated: time.Now(),
		}
		nodeRings[i].AddNode(*node)
		nodes[i] = node
	}

	// Step 2: Start gossip handlers for all nodes
	gossipHandlers := make([]*gossip.GossipHandler, numNodes)
	for i, node := range nodes {
		gossipHandlers[i] = gossip.NewGossipHandler(node, nodeRings[i], gossipFanOut, suspectToDeadTimeout,gossipInterval)

		// Add all nodes to the membership list
		for _, otherNode := range nodes {
			if !node.Equals(otherNode) {
				gossipHandlers[i].Membership.AddOrUpdateNode(otherNode, nodeRings[i])
			}
		}

	}

	// Step 3: Run gRPC servers for all nodes
	servers := []*grpc.Server{}
	contexts := []*context.Context{}
	cancelFuncs := []*context.CancelFunc{}
	for i, handler := range gossipHandlers {
		ctx, cancel := context.WithCancel(context.Background())
		contexts = append(contexts, &ctx)
		cancelFuncs = append(cancelFuncs, &cancel)
		server, err := StartNode(handler)
		if err != nil {
			log.Fatalf("Failed to start gRPC server for node %s: %v", nodes[i].Name, err)
			defer cancel()
			server.Stop()
			return nil, nil, nil, nil, nil, nil 
		}
		servers = append(servers, server)

	}

	// Step 4: Start gossip protocol for each node in separate goroutines
	for i, handler := range gossipHandlers {
		go handler.Start(*contexts[i], gossipFanOut)
	}

	return nodes, gossipHandlers, servers, nodeRings, contexts, cancelFuncs
}

func stopServers(servers []*grpc.Server, cancelContexts []*context.CancelFunc) {
	for i, server := range servers {
		server.Stop()
		defer (*cancelContexts[i])()
	}
}




func TestGossipProtocolIntegration(t *testing.T) {
	// Step 0: Create an initial system with 5 nodes
	numNodes := 3
	numVirtualNodes := uint64(3)
	replicationFactor := 2
	gossipFanOut := 2
	suspectToDeadTimeout := 8
	gossipInterval := 3

	nodes, gossipHandlers, servers, nodeRings, _, cancelContexts := createInitialSystem(numNodes, numVirtualNodes, replicationFactor, gossipFanOut, suspectToDeadTimeout,gossipInterval)

	defer log.Println("TestGossipProtocolIntegration completed")
	defer stopServers(servers, cancelContexts)

	time.Sleep(10 * time.Second)

	// Step 6: Verify membership consistency across all nodes
	expectedClusterSize := len(nodes) // All nodes except the current node
	for _, handler := range gossipHandlers {
		actualClusterSize := len(handler.Membership.GetAllNodes())
		assert.Equal(t, expectedClusterSize, actualClusterSize, "Cluster size mismatch")
	}

	// TODO step 7: verify consistent ring data across all nodes
	for i := 0; i < numNodes; i++ {
		// Get all nodes in the membership
		members := gossipHandlers[i].Membership.GetAllNodes()
		for _, member := range members {
			log.Print("current node ",gossipHandlers[i].NodeInfo.ID,", Node's Ring:",nodeRings[i].String())
			assert.True(t, nodeRings[0].DoesRingContainNode(member), "Node[%s] Node with ID %s not found in ring", gossipHandlers[i].NodeInfo.ID, member.ID)
		}
	}

}

func TestAddNodesToStableSystem(t *testing.T) {
	// create an initial system with 3 nodes
	numNodes := 3
	numVirtualNodes := uint64(3)
	replicationFactor := 2
	gossipFanOut := 2
	suspectToDeadTimeout := 8
	gossipInterval := 3
	
	existingNodes, existingGossipHandlers, servers, nodeRings, _, cancelContext := createInitialSystem(numNodes, numVirtualNodes, replicationFactor, gossipFanOut,suspectToDeadTimeout,gossipInterval)

	defer log.Println("TestGossipProtocolIntegration completed")
	defer stopServers(servers, cancelContext)

	time.Sleep(3 * time.Second)

	// step 6: Run new node server
	newNode := types.Node{
		IPAddress: "localhost",
		ID:     "Node_3",
		Port:   uint64(9003),
		Name:   fmt.Sprintf("localhost:%d", 9003),
		Status: types.NodeStatusAlive,
	}
	newNodeRing := rp.CreateConsistentHashingRing(10, 3)
	newNodeRing.AddNode(newNode)
	newNodeHandler := gossip.NewGossipHandler(&newNode,newNodeRing,gossipFanOut,suspectToDeadTimeout, gossipInterval)
	newServer, newServerErr := StartNode(newNodeHandler)
	if newServerErr != nil {
		t.Fatalf("Failed to start gRPC server for node %s: %v",newNode.Name, newServerErr)
	}
	defer newServer.Stop()

	// step 7: Manually add a random existing node to memberships
	randomExistingNode := existingNodes[0]
	newNodeHandler.Membership.AddOrUpdateNode(randomExistingNode, newNodeRing)

	// Start gossip protocol for the new node
	ctxNewNode, cancelNewNode := context.WithCancel(context.Background())
	go newNodeHandler.Start(ctxNewNode,2)
	defer cancelNewNode()

	// Allow gossip to propagate
	time.Sleep(6 * time.Second)


	// Step 5: Verify that the new node are present in all handlers' memberships
	newNodeID := "Node_3"
	for _, handler := range existingGossipHandlers {
		members := handler.Membership.GetAllNodes()
		found := false

		for _, member := range members {
			if member.ID == newNodeID {
				found = true
				assert.True(t, member.IsAlive(), "New node not marked as alive")
				break
			}
		}

		// Assert that the node with ID "Node_5" exists in the handler's membership
		assert.True(t, found, "Node[%s] Node with ID %s not found in membership", handler.NodeInfo.ID, newNodeID)
	}

	// Step 6: Verify that the new node is present in the ring, and all existing nodes are present in the new node's ring
	existingGossipHandlers = append(existingGossipHandlers, newNodeHandler)
	nodeRings = append(nodeRings, newNodeRing)
	for i , existingGossipHandler := range existingGossipHandlers {
		// Get all nodes in the membership
		members := existingGossipHandler.Membership.GetAllNodes()
		for _, member := range members {
			assert.True(t, nodeRings[i].DoesRingContainNode(member), "Node[%s] Node with ID %s not found in ring", existingGossipHandlers[i].NodeInfo.ID, member.ID)
		}
	}
}

func TestRemoveUnresponsiveNode(t *testing.T){
	// create an initial system with 3 nodes
	numNodes := 3
	nodeToRemoveIndex := numNodes-1
	numVirtualNodes := uint64(3)
	replicationFactor := 2
	gossipFanOut := 2
	suspectToDeadTimeout := 5
	gossipInterval := 3
	
	_, existingGossipHandlers, existingServers, nodeRings, _ , cancelContexts := createInitialSystem(numNodes, numVirtualNodes, replicationFactor, gossipFanOut, suspectToDeadTimeout,gossipInterval)

	defer log.Println("TestGossipProtocolIntegration completed")
	defer stopServers(existingServers, cancelContexts)

	time.Sleep(4 * time.Second)
	existingServers[nodeToRemoveIndex].Stop()
	(*cancelContexts[nodeToRemoveIndex])()
	log.Printf("Simulating failure of node %s", existingGossipHandlers[nodeToRemoveIndex].NodeInfo.Name)

	// step 5: Allow gossip to propagate
	time.Sleep(12 * time.Second)

	// step 6: Verify that the unresponsive node is marked as dead in all handlers' memberships
	for _ , handler := range existingGossipHandlers[:nodeToRemoveIndex] {
		members := handler.Membership.GetAllNodes()
		for _, member := range members {
			if member.ID != fmt.Sprintf("Node_%d", nodeToRemoveIndex) {
				assert.True(t, member.IsAlive(), "Node %s is not marked as alive.", member.ID)
			} else {
				assert.True(t, member.IsDead(), "Node %s is not marked as Dead.", member.ID)
			}
		}
	}

	// step 7: Verify that the unresponsive node is removed from the ring
	for i, handler := range existingGossipHandlers {
		members := handler.Membership.GetAllNodes()
		for _, member := range members {
			assert.True(t, nodeRings[i].DoesRingContainNode(member), "Node[%s] Node with ID %s not found in ring", handler.NodeInfo.ID, member.ID)
		}
	}
}

func TestDeadNodeRecovery(t *testing.T) {
    // Step 1: Create an initial system with 4 nodes
    numNodes := 4
    numVirtualNodes := uint64(1)
    replicationFactor := 2
    gossipFanOut := 2
	suspectToDeadTimeout := 5
	gossipInterval := 3

    nodes, gossipHandlers, servers, _, _, cancelContexts := createInitialSystem(numNodes, numVirtualNodes, replicationFactor, gossipFanOut, suspectToDeadTimeout,gossipInterval)

    // Step 2: Simulate a node failure
    failedNodeIndex := 3
    servers[failedNodeIndex].Stop()
	(*cancelContexts[failedNodeIndex])()
    log.Printf("Simulating failure of node %s", nodes[failedNodeIndex].Name)

    // Allow time for gossip to propagate the failure
    time.Sleep(10 * time.Second)

    // Verify that the failed node is marked as dead in other nodes' memberships
    for _, handler := range gossipHandlers[:failedNodeIndex] {
        members := handler.Membership.GetAllNodes()
        for _, member := range members {
            if member.ID == nodes[failedNodeIndex].ID {
                assert.False(t, member.IsAlive(), "Node %s is still marked as alive", member.ID)
            }
        }
    }

    // Step 3: Simulate the failed node recovering and responding
    recoveredServer, err := StartNode(gossipHandlers[failedNodeIndex])
    if err != nil {
        t.Fatalf("Failed to restart gRPC server for recovered node: %v", err)
    }

	contextNew, cancelNew := context.WithCancel(context.Background())
	go gossipHandlers[failedNodeIndex].Start(contextNew, gossipFanOut)

	cancelContexts[failedNodeIndex] = &cancelNew
    defer recoveredServer.Stop()
	defer stopServers(servers, cancelContexts)
	defer log.Println("TestDeadNodeRecovery completed")

    log.Printf("[%s] is now responding", nodes[failedNodeIndex].ID)

    // Allow time for gossip to propagate the recovery
    time.Sleep(6 * time.Second)

    // Step 4: Verify that the recovered node is marked as alive in other nodes' memberships
    for _, handler := range gossipHandlers[:failedNodeIndex] {
        members := handler.Membership.GetAllNodes()
        found := false
        for _, member := range members {
            if member.ID == nodes[failedNodeIndex].ID {
                found = true
                assert.True(t, member.IsAlive(), "Recovered node %s is not marked as alive", member.ID)
                break
            }
        }
        assert.True(t, found, "Node %s not found in membership after recovery", nodes[failedNodeIndex].ID)
    }
}