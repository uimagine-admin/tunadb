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
	"github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// handle incoming read request
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("Received read request from %s , PageId: %s ,Date: %s, columns: %s", req.Name, req.PageId, req.Date, req.Columns)
	//call read_path

	// Forward the request to node 2 use WithTransportCredentials and insecure.NewCredentials()
	// Create a connection to the other node with secure transport credentials
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second) // Add a timeout for better error handling
	defer cancel()

	conn, err := grpc.NewClient("cassandra-node2:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect to node 2: %v", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	resp, err := client.Read(ctx, req)
	if err != nil {
		log.Fatalf("Could not read from node 2: %v", err)
	}

	return resp, nil

	// //replace below part with reply form read handler
	// return &pb.ReadResponse{
	// 	Date:     "3/11/2024",
	// 	PageId:   "1",
	// 	Columns:  req.Columns,
	// 	Values:   []string{"click", "btn1", "1"}, //for now just retrieve a single row
	// 	Name:     os.Getenv("NODE_NAME"),         //name of node which replied
	// 	NodeType: "IS_NODE",
	// }, nil
}

// handle incoming write request
func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {

	//call write_path
	log.Printf("Received Write request from %s : Date %s PageId %s Event %s ComponentId %s ", req.Name, req.Date, req.PageId, req.Event, req.ComponentId)

	//replace below part with reply
	return &pb.WriteResponse{
		Ack:      true,
		Name:     os.Getenv("NODE_NAME"),
		NodeType: "IS_NODE",
	}, nil
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
