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

func TestGossipProtocolIntegration(t *testing.T) {
	// Step 1: Initialize the cluster with a consistent hashing ring
	numNodes := 5
	nodeRings := []*rp.ConsistentHashingRing{}
	for i := 0; i < numNodes; i++ {
		ring := rp.CreateConsistentHashingRing(10, 3)
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
		}
		nodeRings[i].AddNode(*node)
		nodes[i] = node
	}

	// Step 2: Start gossip handlers for all nodes
	gossipHandlers := make([]*gossip.GossipHandler, numNodes)
	for i, node := range nodes {
		gossipHandlers[i] = gossip.NewGossipHandler(node, nodeRings[i])

		// Add all nodes to the membership list
		for _, otherNode := range nodes {
			if !node.Equals(otherNode) {
				gossipHandlers[i].Membership.AddOrUpdateNode(otherNode)
			}
		}

	}

	// Step 3: Run gRPC servers for all nodes
	servers := []*grpc.Server{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i, handler := range gossipHandlers {
		server, err := StartNode(handler)
		if err != nil {
			t.Fatalf("Failed to start gRPC server for node %s: %v", nodes[i].Name, err)
		}
		servers = append(servers, server)
	}
	defer func() {
		for _, server := range servers {
			server.Stop()
		}
	}()

	// Step 4: Start gossip protocol for each node in separate goroutines
	for _, handler := range gossipHandlers {
		go handler.Start(ctx,2)
	}

	// Step 5: Allow gossip to propagate
	time.Sleep(6 * time.Second)

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
			log.Print("current node",gossipHandlers[i].NodeInfo.ID,", Node's Ring:",nodeRings[i].String())
			assert.True(t, nodeRings[0].DoesRingContainNode(member), "Node[%s] Node with ID %s not found in ring", gossipHandlers[i].NodeInfo.ID, member.ID)
		}
	}

}


func TestAddNodesToStableSystem(t *testing.T) {
	// Step 1: Initialize a stable system with 3 nodes
	numExistingNodes := 3
	nodeRings := []*rp.ConsistentHashingRing{}
	for i := 0; i < numExistingNodes; i++ {
		nodeRings = append(nodeRings, rp.CreateConsistentHashingRing(10, 3)) // 10 virtual nodes, 3 replicas
	}
	existingNodes := make([]*types.Node, numExistingNodes)

	for i := 0; i < numExistingNodes; i++ {
		node := &types.Node{
			IPAddress: "localhost",
			ID:     fmt.Sprintf("Node_%d", i),
			Port:   uint64(9000 + i),
			Name:   fmt.Sprintf("localhost:%d", 9000+i),
			Status: types.NodeStatusAlive,
		}
		nodeRings[i].AddNode(*node)
		existingNodes[i] = node
	}

	// Step 2: Set up GossipHandlers for existing nodes
	existingGossipHandlers := make([]*gossip.GossipHandler, numExistingNodes)
	for i, node := range existingNodes {
		handler := gossip.NewGossipHandler(node, nodeRings[i])
		for _, otherNode := range existingNodes {
			if !node.Equals(otherNode) {
				handler.Membership.AddOrUpdateNode(otherNode)
			}
		}
		existingGossipHandlers[i] = handler
	}

	// Step 3: Run gRPC servers for all nodes
	existingServers := []*grpc.Server{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i, handler := range existingGossipHandlers {
		server, err := StartNode(handler)
		if err != nil {
			t.Fatalf("Failed to start gRPC server for node %s: %v", existingNodes[i].Name, err)
		}
		existingServers = append(existingServers, server)
	}
	defer func() {
		for _, server := range existingServers {
			server.Stop()
		}
	}()

	// Step 4: Start gossip protocol for each node in separate goroutines
	for _, handler := range existingGossipHandlers {
		go handler.Start(ctx, 2)
	}

	// step 5: Allow gossip to propagate
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
	newNodeHandler := gossip.NewGossipHandler(&newNode,newNodeRing)
	newServer, newServerErr := StartNode(newNodeHandler)
	if newServerErr != nil {
		t.Fatalf("Failed to start gRPC server for node %s: %v",newNode.Name, newServerErr)
	}
	defer newServer.Stop()

	// step 7: Manually add a random existing node to memberships
	randomExistingNode := existingNodes[0]
	newNodeHandler.Membership.AddOrUpdateNode(randomExistingNode)

	// Start gossip protocol for the new node
	go newNodeHandler.Start(ctx,2)

	// Allow gossip to propagate
	time.Sleep(6 * time.Second)


	// Step 5: Verify that the new node are present in all handlers' memberships
	newNodeID := "Node_3"
	for _, handler := range existingGossipHandlers {
		members := handler.Membership.GetAllNodes()
		found := false

		for _, member := range members {
			if member.ID == newNodeID { // Check if the node ID matches "Node_5"
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
	// step 1: step up a stable system with 5 nodes 
	numExistingNodes := 4
	nodeRings := []*rp.ConsistentHashingRing{}
	for i := 0; i < numExistingNodes; i++ {
		nodeRings = append(nodeRings, rp.CreateConsistentHashingRing(10, 3)) // 10 virtual nodes, 3 replicas
	}
	existingNodes := make([]*types.Node, numExistingNodes)

	for i := 0; i < numExistingNodes; i++ {
		node := &types.Node{
			IPAddress: "localhost",
			ID:     fmt.Sprintf("Node_%d", i),
			Port:   uint64(9000 + i),
			Name:   fmt.Sprintf("localhost:%d", 9000+i),
			Status: types.NodeStatusAlive,
		}
		nodeRings[i].AddNode(*node)
		existingNodes[i] = node
	}

	// Step 2: Set up GossipHandlers for existing nodes
	existingGossipHandlers := make([]*gossip.GossipHandler, numExistingNodes)
	for i, node := range existingNodes {
		handler := gossip.NewGossipHandler(node, nodeRings[i])
		for _, otherNode := range existingNodes {
			if !node.Equals(otherNode) {
				handler.Membership.AddOrUpdateNode(otherNode)
			}
		}
		existingGossipHandlers[i] = handler
	}

	// Step 3: Run gRPC servers for all nodes
	existingServers := []*grpc.Server{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i, handler := range existingGossipHandlers {
		server, err := StartNode(handler)
		if err != nil {
			t.Fatalf("Failed to start gRPC server for node %s: %v", existingNodes[i].Name, err)
		}
		existingServers = append(existingServers, server)
	}
	defer func() {
		for i, server := range existingServers {
			if i == 3 {
				continue
			}
			server.Stop()
		}
	}()

	// stop node 5: server after 6 seconds
	go func() {
		time.Sleep(3 * time.Second)
		existingServers[3].Stop()
	}()

	// Step 4: Start gossip protocol for each node in separate goroutines
	for _, handler := range existingGossipHandlers {
		go handler.Start(ctx, 2)
	}

	// step 5: Allow gossip to propagate
	time.Sleep(15 * time.Second)

	// step 6: Verify that the unresponsive node is marked as dead in all handlers' memberships
	for _ , handler := range existingGossipHandlers[:3] {
		members := handler.Membership.GetAllNodes()
		for _, member := range members {
			if member.ID != "Node_3" {
				assert.True(t, member.IsAlive(), "Node %s is not marked as alive", member.ID)
			} else {
				assert.False(t, member.IsAlive(), "Node %s is still marked as alive", member.ID)
			}
		}
	}
}