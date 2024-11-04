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

//receiver of request?
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("Received read request from %s , PageId: %s ,Date: %s, columns: %s",req.Name, req.PageId,req.Date,req.Columns)
	//if not coordinator:find from memtable , if not then SS table

	//if coordinator : hash -> send to other nodes , await reply , quorum

	//reply client if coordinator /coordinator if normal node
	return &pb.ReadResponse{
		Date: "3/11/2024",
		PageId: "1",      
		Columns: req.Columns,
		Values:   []string{"click","btn1","1"},
		Name: os.Getenv("NODE_NAME"), //name of node which replied
	}, nil
}

func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	//write logic would be here: 
	//if coordinator : hash -> send to other nodes , await reply , if crash detected->repair
	//if not coordinator : wirte to commitlog->memtable->check if memtable>capacity ->SStable
	log.Printf("Received Write request from %s : Date %s PageId %s Event %s ComponentId %s ",req.Name,req.Date,req.PageId,req.Event,req.ComponentId)

	//reply client if coordinator /coordinator if normal node
	return &pb.WriteResponse{
		Ack:   true,
		Name: os.Getenv("NODE_NAME"),
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

