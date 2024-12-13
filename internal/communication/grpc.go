package communication

import (
	"context"
	"log"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"google.golang.org/grpc"
)

func SendRead(Ctx *context.Context, address string, req *pb.ReadRequest) (*pb.ReadResponse, error) {
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
	log.Printf("Received read response . Name: %s, pageID: %s , cols:%s, RowData \n", resp.Name, resp.PageId, resp.Columns)
	for i, row := range resp.Rows {
		log.Printf("	Row %d: %v\n", i, row)
	}
	return resp, err
}

func SendWrite(Ctx *context.Context, address string, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	log.Printf("sending write request to %s \n", address)
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
		log.Fatalf("Could not send write to address %s: %v", address, err)
	}
	log.Printf("Received write response , Ack:  %v from %s \n", resp.Ack, resp.Name)
	return resp, err
}

func CheckReadIsFromNode(req *pb.ReadRequest) bool {
	if req.NodeType != "IS_NODE" {
		return false
	} else {
		return true
	}
}
func CheckWriteIsFromNode(req *pb.WriteRequest) bool {
	if req.NodeType != "IS_NODE" {
		return false
	} else {
		return true
	}
}

/*
	sendGossipMessage sends a gossip message to a target node using gRPC.
	Args: 
		address: Refers the full address of <ip_address>:<port>
*/
func SendGossipMessage(Ctx *context.Context, address string, req *pb.GossipMessage) error {
	//test
	// log.Printf("sending gossip message to %s \n", address)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	_, err = client.Gossip(ctx, req)
	return err
}

func SendDelete(Ctx *context.Context, address string, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Printf("sending delete request to %s \n", address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Did not connect to node: %v", err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	//additional configuration can pass in as param
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.Delete(ctx, req)

	if err != nil {
		log.Fatalf("Could not send write to address %s: %v", address, err)
	}
	log.Printf("Received delete response , Ack:  %v, Message: %v from %s \n", resp.Success, resp.Message, resp.Name)
	return resp, err
}

func SendBulkWrite(Ctx *context.Context, address string, req *pb.BulkWriteRequest) (*pb.BulkWriteResponse, error) {
	log.Printf("sending bulk write request to %s \n", address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Did not connect to node: %v", err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	//additional configuration can pass in as param
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.BulkWrite(ctx, req)

	if err != nil {
		log.Fatalf("Could not send bulk write to address %s: %v", address, err)
	}
	log.Printf("Received bulk write response , Ack:  %v from %s \n", resp.Ack, resp.Name)
	return resp, err
}