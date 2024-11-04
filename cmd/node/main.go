package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"

	"google.golang.org/grpc"
)

// server is used to implement cassandragrpc.CassandraServiceServer.
type server struct {
	pb.UnimplementedCassandraServiceServer
}

//receiver of request?
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("Received request , partition key: %s , columns: %s", req.PartitionKey,req.Columns)
	return &pb.ReadResponse{
		PartitionKey: "1",
		Columns:      []string{"sample1"},
		Values:       []string{""},
	}, nil
}

func main() {
	// Start gRPC server
	go startServer()

	// Add a sleep to allow server startup
	time.Sleep(2 * time.Second)

	// Simulate peer communication
	if os.Getenv("PEER_ADDRESS") != "" {
		sendRead(os.Getenv("PEER_ADDRESS"))
	}

	// Block forever to keep the node running
	select {}
}

//server listening
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

//client sending to server
func sendRead(peerAddress string) {
	conn, err := grpc.Dial(peerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Did not connect to peer: %v", err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.Read(ctx, &pb.ReadRequest{
		PartitionKey:"1",
		Columns:      []string{"sample1"},})

	if err != nil {
		log.Fatalf("Could not greet peer: %v", err)
	}
	fmt.Printf("Received resp from peer.Partition Key %s , values: %s \n",resp.PartitionKey,resp.Values)
}
