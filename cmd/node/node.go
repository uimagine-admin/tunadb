package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/coordinator"
	"github.com/uimagine-admin/tunadb/internal/gossip"
	"github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedCassandraServiceServer
	GossipHandler *gossip.GossipHandler
}

var portInternal = os.Getenv("INTERNAL_PORT")
var peerAddresses = getPeerAddresses()

// handle incoming read request
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("Received read request from %s , PageId: %s ,Date: %s, columns: %s", req.Name, req.PageId, req.Date, req.Columns)
	ring, err := startRing(peerAddresses)
	if err != nil {
		return &pb.ReadResponse{}, err
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(ring, currentNode)
	ctx_read, _ := context.WithTimeout(context.Background(), time.Second)
	//call read_path
	resp, err := c.Read(ctx_read, req)
	if err != nil {
		return &pb.ReadResponse{}, err
	}

	return resp, nil
}

// handle incoming write request
func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	//call write_path
	log.Printf("Received Write request from %s : Date %s PageId %s Event %s ComponentId %s ", req.Name, req.Date, req.PageId, req.Event, req.ComponentId)
	ring, err := startRing(peerAddresses)
	if err != nil {
		return &pb.WriteResponse{}, err
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(ring, currentNode)
	//call write_path
	ctx_write, _ := context.WithTimeout(context.Background(), time.Second)
	resp, err := c.Write(ctx_write, req)
	if err != nil {
		return &pb.WriteResponse{}, err
	}

	return resp, nil
}

// handle incoming gossip request
func (s *server) Gossip(ctx context.Context, req *pb.GossipMessage) (*pb.GossipAck, error) {
	return s.GossipHandler.HandleGossipMessage(ctx, req)
}

// handle delete requests
func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Printf("Received delete request: Date %s PageId %s Event %s ComponentId %s ", req.Date, req.PageId, req.Event, req.ComponentId)
	ring, err := startRing(peerAddresses)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: "Failed to start ring",
		}, err
	}

	var portnum, _ = strconv.ParseUint("50051", 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(ring, currentNode)
	//call delete_path
	ctx_delete, _ := context.WithTimeout(context.Background(), time.Second)
	resp, err := c.Delete(ctx_delete, req)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	return resp, nil
}

func main() {
	// Start the gossip protocol and gRPC server
	nodeName := os.Getenv("NODE_NAME")
	nodeID := os.Getenv("ID")
	port, _ := strconv.ParseUint(portInternal, 10, 64)

	// Set up the consistent hashing ring
	ring, err := startRing(peerAddresses)
	if err != nil {
		log.Fatalf("Failed to start ring: %v", err)
	}

	// Initialize the current node and gossip handler
	currentNode := &types.Node{
		ID:   nodeID,
		Name: nodeName,
		Port: port,
		Status: types.NodeStatusAlive,
		IPAddress: fmt.Sprintf("%s:%s", nodeName, portInternal),
	}

	// Adjust Fanout, timeouts, and interval as needed
	gossipFanOut := 2
	gossipTimeout := 8
	gossipInterval := 3
	gossipHandler := gossip.NewGossipHandler(currentNode, ring, gossipFanOut, gossipTimeout, gossipInterval) 

	// Add peers to the membership list
	for _, address := range peerAddresses {
		parts := strings.Split(address, ":")
		port, _ := strconv.ParseUint(parts[1], 10, 64)
		peerNode := &types.Node{
			Name: parts[0],
			Port: port,
			ID:   fmt.Sprintf("node-%s", parts[0][len(parts[0])-1:]),
			IPAddress: address,
		}
		gossipHandler.Membership.AddOrUpdateNode(peerNode, ring)
	}

	// Start gRPC server
	go startServer(gossipHandler)

	// Start the gossip protocol
	go gossipHandler.Start(context.Background(), 2)

	// Block forever
	select {}
}

func startServer(gossipHandler *gossip.GossipHandler) {
	log.Printf("Listening on %s", fmt.Sprintf(":%s", portInternal))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", portInternal))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterCassandraServiceServer(grpcServer, &server{
		GossipHandler: gossipHandler,
	})

	log.Printf("gRPC server is running on port %s", portInternal)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func startRing(peerAddresses []string) (*ring.ConsistentHashingRing, error) {
	nodes := make([]types.Node, len(peerAddresses))
	for i, address := range peerAddresses {
		parts := strings.Split(address, ":")
		port, _ := strconv.ParseUint(parts[1], 10, 64)
		nodes[i] = types.Node{
			Name: parts[0],
			Port: port,
			ID:   fmt.Sprintf("node-%d", i),
			IPAddress: address,
		}
	}

	r := ring.CreateConsistentHashingRing(uint64(len(nodes)), 2)
	for _, node := range nodes {
		r.AddNode(node)
	}

	return r, nil
}

func getPeerAddresses() []string {
	// Fetch peer addresses from environment variables
	peerAddressesEnv := os.Getenv("PEER_NODES")
	if peerAddressesEnv == "" {
		log.Fatal("PEER_ADDRESSES environment variable not set")
	}

	// Split addresses into a slice
	peerAddresses := strings.Split(peerAddressesEnv, ",")
	return peerAddresses
}