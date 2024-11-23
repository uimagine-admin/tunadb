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
		go handler.Start(ctx)
	}

	// Step 5: Allow gossip to propagate
	time.Sleep(10 * time.Second)

	// Step 6: Verify membership consistency across all nodes
	expectedClusterSize := len(nodes) // All nodes except the current node
	for _, handler := range gossipHandlers {
		actualClusterSize := len(handler.Membership.GetAllNodes())
		assert.Equal(t, expectedClusterSize, actualClusterSize, "Cluster size mismatch")
	}

	// TODO step 7: verify consistent ring data across all nodes
}


func TestAddNodesToStableSystem(t *testing.T) {
	// Step 1: Initialize a stable system with 3 nodes
	numExistingNodes := 5
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
		go handler.Start(ctx)
	}

	// step 5: Allow gossip to propagate
	time.Sleep(3 * time.Second)

	// step 6: Run new node server
	newNode := types.Node{
		IPAddress: "localhost",
		ID:     "Node_5",
		Port:   uint64(9005),
		Name:   fmt.Sprintf("localhost:%d", 9005),
		Status: types.NodeStatusAlive,
	}
	newNodeRing := rp.CreateConsistentHashingRing(10, 3)
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
	go newNodeHandler.Start(ctx)

	// Allow gossip to propagate
	time.Sleep(15 * time.Second)


	// Step 5: Verify that the new node are present in all handlers' memberships
	newNodeID := "Node_5"
	for _, handler := range existingGossipHandlers {
		members := handler.Membership.GetAllNodes()
		found := false

		for _, member := range members {
			// log.Print(handler.NodeInfo.ID,member.ID)
			if member.ID == newNodeID { // Check if the node ID matches "Node_5"
				found = true
				assert.True(t, member.IsAlive(), "New node not marked as alive")
				break
			}
		}

		// Assert that the node with ID "Node_5" exists in the handler's membership
		assert.True(t, found, "Node[] Node with ID %s not found in membership", newNodeID)
	}


	// // Step 6: Verify consistent data across memberships
	// for _, handler := range append(existingGossipHandlers, newNodeHandler) {
	// 	members := handler.Membership.GetAllNodes()
	// 	for _, otherHandler := range existingGossipHandlers {
	// 		assert.Equal(t, members, otherHandler.Membership.GetAllNodes(), "Membership data inconsistency")
	// 	}
	// }
}
