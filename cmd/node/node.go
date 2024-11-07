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
	"github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
	"google.golang.org/grpc"
)

// server is used to implement cassandragrpc.CassandraServiceServer.
type server struct {
	pb.UnimplementedCassandraServiceServer
}

var peerAddresses = []string{
	"cassandra-node1:50051",
	"cassandra-node2:50051",
	"cassandra-node3:50051",
}

// port for internal communication
var portInternal = "50051"

// handle incoming read request
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("Received read request from %s , PageId: %s ,Date: %s, columns: %s", req.Name, req.PageId, req.Date, req.Columns)
	ring, err := startRing(peerAddresses)
	if err != nil {
		return &pb.ReadResponse{}, err
	}

	var portnum, _ = strconv.ParseUint("50051", 10, 64)
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

	var portnum, _ = strconv.ParseUint("50051", 10, 64)
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

func main() {
	// Hardcoded addresses for nodes
	// nodeAddress := os.Getenv("NODE_ADDRESS")

	// ring, err := startRing(peerAddresses)
	// if err != nil {
	// 	log.Fatalf("Failed to start ring: %v", err)
	// }

	// Start gRPC server
	go startServer()

	// Add a sleep to allow server startup
	time.Sleep(2 * time.Second)

	// Block forever to keep the node running
	select {}
}

// server listening
func startServer() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterCassandraServiceServer(grpcServer, &server{})
	log.Println("gRPC server is running on port 50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func startRing(peerAddresses []string) (*ring.ConsistentHashingRing, error) {
	// create the nodes
	nodes := make([]types.Node, len(peerAddresses))
	for i, address := range peerAddresses {
		addressParts := strings.Split(address, ":")
		port, err := strconv.ParseUint(addressParts[1], 10, 64)
		if err != nil {
			return &ring.ConsistentHashingRing{}, err
		}
		nodes[i] = types.Node{
			Name:      addressParts[0],
			Port:      port,
			IPAddress: "", // we dont use it
			ID:        fmt.Sprintf("node-%d", i),
		}
	}

	// Start the ring
	ring := ring.CreateConsistentHashingRing(uint64(len(nodes)), 2)

	for _, node := range nodes {
		ring.AddNode(node)
	}

	return ring, nil
}
