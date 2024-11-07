package main

import (
	"context"
	"os"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/communication"
)

func main() {
	// Add a sleep to allow server startup
	time.Sleep(2 * time.Second)

	//peer address : random node address
	// Simulate peer communication
	if os.Getenv("PEER_ADDRESS") != "" {
		// sendRead(os.Getenv("PEER_ADDRESS"))

		ctx, _ := context.WithTimeout(context.Background(), time.Second)

		communication.SendRead(&ctx, os.Getenv("PEER_ADDRESS"), &pb.ReadRequest{
			Date:     "3/11/2024",
			PageId:   "1",
			Columns:  []string{"event", "componentId", "count"},
			Name:     os.Getenv("NODE_NAME"),
			NodeType: "IS_CLIENT",
		})

		// ctx_write, _ := context.WithTimeout(context.Background(), time.Second)
		// // sendWrite(os.Getenv("PEER_ADDRESS"))
		// communication.SendWrite(&ctx_write, os.Getenv("PEER_ADDRESS"), &pb.WriteRequest{
		// 	Date:        "3/11/2024",
		// 	PageId:      "1",
		// 	Event:       "click",
		// 	ComponentId: "btn1",
		// 	Name:        os.Getenv("NODE_NAME"),
		// 	NodeType:    "IS_CLIENT"})

		// Block forever to keep the node running
		select {}
	}
}
