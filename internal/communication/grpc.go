package communication

import (
	"context"
	"log"

	pb "github.com/uimagine-admin/tunadb/api"
	"google.golang.org/grpc"
)

var Reset = "\033[0m"						// Reset
// Other Shared Colors
var GeneralError = "\033[38;5;196m"          // Bright Red
var GeneralInfo = "\033[38;5;33m"            // Blue
var GeneralSuccess = "\033[38;5;40m"         // Green
var GeneralDebug = "\033[38;5;99m"           // Purple

/*
 sendRead sends a read request to a target node using gRPC.
 Args:
	address: Refers the full address of <ip_address>:<port>
*/
func SendRead(Ctx *context.Context, address string, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("connecting and sending read request \n")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf(GeneralError + "Did not connect to node: %v" + Reset, err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)

	//additional configuration can pass in as param
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	resp, err := client.Read(ctx, req)

	if err != nil {
		log.Printf(GeneralError + "Could not read peer: %v" + Reset , err)
	}
	log.Printf(GeneralSuccess + "Received read response . Name: %s, pageID: %s , cols:%s, RowData \n" + Reset, resp.Name, resp.PageId, resp.Columns)
	for i, row := range resp.Rows {
		log.Printf("	Row %d: %v\n", i, row)
	}
	return resp, err
}

/*
 sendWrite sends a write request to a target node using gRPC.
 Args:
	address: Refers the full address of <ip_address>:<port>
*/
func SendWrite(Ctx *context.Context, address string, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	log.Printf(GeneralInfo + "Sending write request to %s \n" + Reset, address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf(GeneralError + "Did not connect to node: %v" + Reset, err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	//additional configuration can pass in as param
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Write(ctx, req)

	if err != nil {
		log.Printf(GeneralError + "Could not send write to address %s: %v" + Reset, address, err)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = client.Gossip(ctx, req)
	return err
}

func SendDelete(Ctx *context.Context, address string, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Printf("Sending delete request to %s \n", address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf(GeneralError + "Did not connect to node: %v" + Reset, err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	//additional configuration can pass in as param
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.Delete(ctx, req)

	if err != nil {
		log.Printf(GeneralError + "Could not send write to address %s: %v" + Reset, address, err)
	}
	log.Printf("Received delete response , Ack:  %v, Message: %v from %s \n", resp.Success, resp.Message, resp.Name)
	return resp, err
}

func SendBulkWrite(Ctx *context.Context, address string, req *pb.BulkWriteRequest) (*pb.BulkWriteResponse, error) {
	log.Printf("Sending bulk write request to %s \n", address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf(GeneralError + "Did not connect to node: %v" + Reset, err)
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)
	//additional configuration can pass in as param
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := client.BulkWrite(ctx, req)

	if err != nil {
		log.Printf(GeneralError + "Could not send bulk write to address %s: %v" + Reset, address, err)
	}
	log.Printf("Received bulk write response , Ack:  %v from %s \n", resp.Ack, resp.Name)
	return resp, err
}