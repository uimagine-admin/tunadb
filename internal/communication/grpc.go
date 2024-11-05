package main

import (
	"context"
	"log"
	"time"
	pb "github.com/uimagine-admin/tunadb/api"
	"google.golang.org/grpc"
)



func sendRead(Ctx Context, address string, req *pb.ReadRequest) {
	log.Printf("connecting and sending read request \n")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Did not connect to node: %v", err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	
	//additional configuration can pass in as param
	ctx, cancel := context.WithTimeout(context.Background(), time.Second) 
	
	defer cancel()
	

	resp, err := client.Read(ctx, req)

	if err != nil {
		log.Fatalf("Could not read peer: %v", err)
	}
	log.Printf("sendRead: Received read response . Name: %s, pageID: %s , cols:%s , values: %s \n",resp.Name,resp.PageId,resp.Columns,resp.Values)
}


func sendWrite(Ctx Context, address string, req *pb.WriteRequest) {
	log.Printf("connecting and sending write request\n")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Did not connect to node: %v", err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	//additional configuration can pass in as param
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	
	resp, err := client.Write(ctx, req)

	if err != nil {
		log.Fatalf("Could not send write to peer: %v", err)
	}
	log.Printf("Received write response , Ack:  %v from %s \n",resp.Ack,resp.Name)
}