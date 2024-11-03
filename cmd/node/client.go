package main

import (
	"context"
	"log"
	"os"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"

	"google.golang.org/grpc"
)



func main() {
	

	// Add a sleep to allow server startup
	time.Sleep(2 * time.Second)

	//peer address should be the coordinator address - need to find a way to get it
	// Simulate peer communication
	if os.Getenv("PEER_ADDRESS") != "" {
		sendRead(os.Getenv("PEER_ADDRESS"))
		sendWrite(os.Getenv("PEER_ADDRESS"))
	}

	// Block forever to keep the node running
	select {}
}



//client sending to server
func sendRead(peerAddress string) {
	log.Printf("connecting and sending read request \n")
	conn, err := grpc.Dial(peerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Did not connect to node: %v", err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	

	resp, err := client.Read(ctx, &pb.ReadRequest{
		PartitionKey:"1",
		Columns:      []string{"col1"},
		Name: os.Getenv("NODE_NAME"),
	})

	if err != nil {
		log.Fatalf("Could not read peer: %v", err)
	}
	log.Printf("Received response from %s. partition key: %s ,cols:%s , values: %s \n",resp.Name,resp.PartitionKey,resp.Columns,resp.Values)
}

//client sending to server
func sendWrite(peerAddress string) {
	log.Printf("connecting and sending write request\n")
	conn, err := grpc.Dial(peerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Did not connect to node: %v", err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	
	resp, err := client.Write(ctx, &pb.WriteRequest{
		PartitionKey:"1",
		Columns:      []string{"col1"},
		Values: []string{"col1 new value"},
		Name: os.Getenv("NODE_NAME"),
	})

	if err != nil {
		log.Fatalf("Could not send write to peer: %v", err)
	}
	log.Printf("Received response , Ack:  %v from %s \n",resp.Ack,resp.Name)
}