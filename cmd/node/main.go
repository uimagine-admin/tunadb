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

// SayHello implements cassandragrpc.CassandraServiceServer
func (s *server) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	log.Printf("Received request from: %s", req.Name)
	return &pb.HelloResponse{Message: "Hello, " + req.Name + " from " + os.Getenv("NODE_NAME")}, nil
}

func main() {
	// Start gRPC server
	go startServer()

	// Add a sleep to allow server startup
	time.Sleep(2 * time.Second)

	// Simulate peer communication
	if os.Getenv("PEER_ADDRESS") != "" {
		sendHelloToPeer(os.Getenv("PEER_ADDRESS"))
	}

	// Block forever to keep the node running
	select {}
}

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

func sendHelloToPeer(peerAddress string) {
	conn, err := grpc.Dial(peerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Did not connect to peer: %v", err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.SayHello(ctx, &pb.HelloRequest{Name: os.Getenv("NODE_NAME")})
	if err != nil {
		log.Fatalf("Could not greet peer: %v", err)
	}
	fmt.Printf("Received greeting from peer: %s\n", resp.Message)
}
