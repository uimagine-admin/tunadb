package main

import (
	"context"
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

//handle incoming read request
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("Received read request from %s , PageId: %s ,Date: %s, columns: %s",req.Name, req.PageId,req.Date,req.Columns)
	//call read_path

	//replace below part with reply form read handler
	return &pb.ReadResponse{
		Date: "3/11/2024",
		PageId: "1",      
		Columns: req.Columns,
		Values:   []string{"click","btn1","1"}, //for now just retrieve a single row
		Name: os.Getenv("NODE_NAME"), //name of node which replied
		NodeType:"IS_NODE",
	}, nil
}

//handle incoming write request
func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	

	//call write_path
	log.Printf("Received Write request from %s : Date %s PageId %s Event %s ComponentId %s ",req.Name,req.Date,req.PageId,req.Event,req.ComponentId)

	//replace below part with reply
	return &pb.WriteResponse{
		Ack:   true,
		Name: os.Getenv("NODE_NAME"),
		NodeType:"IS_NODE",
	}, nil
}

func main() {
	// Start gRPC server
	go startServer()

	// Add a sleep to allow server startup
	time.Sleep(2 * time.Second)

	

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

