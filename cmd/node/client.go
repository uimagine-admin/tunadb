package main

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/communication"
)

func main() {	
	// Add a sleep to allow server startup
	time.Sleep(2 * time.Second)

	//peer address : random node address
	if os.Getenv("PEER_ADDRESS") != "" {
		// sendRead(os.Getenv("PEER_ADDRESS"))

		ctx_write, _ := context.WithTimeout(context.Background(), 5*time.Second)
		// sendWrite(os.Getenv("PEER_ADDRESS"))
		communication.SendWrite(&ctx_write, os.Getenv("PEER_ADDRESS"), &pb.WriteRequest{
			Date:        "2024-11-27T10:00:40.999999999Z",
			PageId:      "1",
			Event:       "click",
			ComponentId: "btn1",
			Name:        os.Getenv("NODE_NAME"),
			NodeType:    "IS_CLIENT"})

		time.Sleep(3 * time.Second)

		// ctx_delete, _ := context.WithTimeout(context.Background(), 5*time.Second)
		// communication.SendDelete(&ctx_delete, os.Getenv("PEER_ADDRESS"), &pb.DeleteRequest{
		// 	Date:        "",
		// 	PageId:      "1",
		// 	Event:       "",
		// 	ComponentId: "btn1",
		// })

		// time.Sleep(3 * time.Second)

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		communication.SendRead(&ctx, os.Getenv("PEER_ADDRESS"), &pb.ReadRequest{
			Date:     "2024-11-27T10:00:40.999999999Z",
			PageId:   "1",
			Columns:  []string{"event", "componentId", "count"},
			Name:     os.Getenv("NODE_NAME"),
			NodeType: "IS_CLIENT",
		})

		// Block forever to keep the node running
		// select {}

		for i := 0; i < 100; i++ {
			time.Sleep(200 * time.Millisecond)
			currentDate := time.Now().Format(time.RFC3339Nano)
			randomPageID := strconv.Itoa(rand.Intn(100)) 
			communication.SendWrite(&ctx_write, os.Getenv("PEER_ADDRESS"), &pb.WriteRequest{
				Date:        currentDate,
				PageId:      randomPageID,
				Event:       "click",
				ComponentId: "btn1",
				Name:        os.Getenv("NODE_NAME"),
				NodeType:    "IS_CLIENT"})

		}

		select{

		}

	}
}
