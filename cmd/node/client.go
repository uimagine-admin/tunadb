package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/communication"
)

// ANSI color codes for terminal output
var Reset = "\033[0m"

var ClientReadRequest = "\033[38;5;240m"  // Dark Gray
var ClientWriteRequest = "\033[38;5;241m" // Darker Blue
var ClientDeleteRequest = "\033[38;5;242m" // Dark Olive Green
var ClientBulkWriteRequest = "\033[38;5;243m" // Dark Steel Blue

var ClusterMembershipRequest = "\033[38;5;208m" // Orange
var ClusterMembershipResponse = "\033[38;5;214m" // Light Orange

func main() {	
	// Wait for 5 seconds to allow the cluster memberships to update
	time.Sleep(5 * time.Second)

	// Initialize cluster membership with the first node
	initialNode := os.Getenv("PEER_ADDRESS")
	if initialNode == "" {
		log.Println(ClusterMembershipRequest + "[Client] PEER_ADDRESS environment variable not set."+ Reset)
		return
	} else {
		// Example write call 
		ctx_write, _ := context.WithCancel(context.Background())
		communication.SendWrite(&ctx_write, os.Getenv("PEER_ADDRESS"), &pb.WriteRequest{
			Date:        "2024-11-27T10:00:40.999999999Z",
			PageId:      "1",
			Event:       "click",
			ComponentId: "btn1",
			Name:        os.Getenv("NODE_NAME"),
			NodeType:    "IS_CLIENT"})
		log.Printf(ClientWriteRequest + "[Client] Sent write request to %s\n" + Reset, os.Getenv("PEER_ADDRESS"))
		time.Sleep(2 * time.Second)

		// Example read call	
		ctx_read_1, _ := context.WithTimeout(context.Background(), 5*time.Second)
		communication.SendRead(&ctx_read_1, os.Getenv("PEER_ADDRESS"), &pb.ReadRequest{
			Date:     "2024-11-27T10:00:40.999999999Z",
			PageId:   "1",
			Columns:  []string{"event", "componentId", "count"},
			Name:     os.Getenv("NODE_NAME"),
			NodeType: "IS_CLIENT",
		})
		log.Printf(ClientReadRequest + "[Client] Sent read request to %s\n" + Reset, os.Getenv("PEER_ADDRESS"))

		// Example delete call
		ctx_delete, _ := context.WithTimeout(context.Background(), 5*time.Second)
		communication.SendDelete(&ctx_delete, os.Getenv("PEER_ADDRESS"), &pb.DeleteRequest{
			Date:        "",
			PageId:      "1",
			Event:       "",
			ComponentId: "btn1",
		})
		log.Printf(ClientDeleteRequest + "[Client] Sent delete request to %s\n" + Reset, os.Getenv("PEER_ADDRESS"))
		time.Sleep(1 * time.Second)

		// Example read call
		ctx_read2, _ := context.WithTimeout(context.Background(), 5*time.Second)
		communication.SendRead(&ctx_read2, os.Getenv("PEER_ADDRESS"), &pb.ReadRequest{
			Date:     "2024-11-27T10:00:40.999999999Z",
			PageId:   "1",
			Columns:  []string{"event", "componentId", "count"},
			Name:     os.Getenv("NODE_NAME"),
			NodeType: "IS_CLIENT",
		})
		log.Printf(ClientReadRequest + "[Client] Sent read request to %s\n" + Reset, os.Getenv("PEER_ADDRESS"))
		time.Sleep(1 * time.Second)

		// Test creation of multiple write requests
		for i := 0; i < 400; i++ {
			ctx_write_i, _ := context.WithTimeout(context.Background(), 5 * time.Second)
			time.Sleep(200 * time.Millisecond)
			currentDate := time.Now().Format(time.RFC3339Nano)
			randomPageID := strconv.Itoa(rand.Intn(100)) 
			communication.SendWrite(&ctx_write_i, os.Getenv("PEER_ADDRESS"), &pb.WriteRequest{
				Date:        currentDate,
				PageId:      randomPageID,
				Event:       "click",
				ComponentId: "btn1",
				Name:        os.Getenv("NODE_NAME"),
				NodeType:    "IS_CLIENT"})

		}
	}

	// Block forever to keep the node running
	select{

	}

}