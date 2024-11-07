package coordinator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/communication"
	"github.com/uimagine-admin/tunadb/internal/db"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// Handles client Read request. fetches the value for a given key with quorum-based consistency.
func (h *CoordinatorHandler) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	if req.NodeType == "IS_NODE" {
		fmt.Printf("simulating read from db for pageID %s\n", req.PageId)
		row, err := db.HandleRead(h.GetNode().ID, req) // TODO: Change Rows into Columns and Values - @jaytaykay

		columns := []string{"Date", "PageId", "Event", "ComponentId"}
		values := []string{"2021-09-01T00:00:00Z", req.PageId, "click", "component1"}

		return &pb.ReadResponse{
			Date:    values[0],
			PageId:  req.PageId,
			Columns: columns,
			Values:  values,
		}, nil
	} else {

		fmt.Printf("Received read request from %s , PageId: %s ,Date: %s, columns: %s\n", req.Name, req.PageId, req.Date, req.Columns)
		ring := h.GetRing()
		replicas := ring.GetNodes(req.PageId)
		if len(replicas) == 0 {
			return &pb.ReadResponse{}, errors.New("no available node for key")
		}

		resultsChan := make(chan *pb.ReadResponse, len(replicas))
		wg := sync.WaitGroup{}

		// for each replica: i'll send an internal read request
		for _, replica := range replicas {
			wg.Add(1)
			// if the replica is the current node, skip it
			if replica.Name == os.Getenv("NODE_NAME") {
				fmt.Printf("simulating read from db for pageID %s\n", req.PageId)
				row, err := db.HandleRead(h.GetNode().ID, req) // TODO: Change Rows into Columns and Values - @jaytaykay

				columns := []string{"Date", "PageId", "Event", "ComponentId"}
				values := []string{"2021-09-01T00:00:00Z", req.PageId, "click", "component1"}
				fmt.Printf("reading rows and cols from db %s , %s\n", values, columns)
				continue
			}

			go func(replica types.Node) {
				defer wg.Done()

				address := fmt.Sprintf("%s:%d", replica.Name, replica.Port)

				ctx_read, _ := context.WithTimeout(context.Background(), time.Second)
				//send the read request with the key
				_, err := communication.SendRead(&ctx_read, address, &pb.ReadRequest{
					Date:     req.Date,
					PageId:   req.PageId,
					Name:     os.Getenv("NODE_NAME"),
					Columns:  []string{"event", "componentId", "count"},
					NodeType: "IS_NODE",
				})

				if err != nil {
					fmt.Printf("error reading from %s: %v\n", address, err)
					return
				}

				// get the reply
				// resultsChan <- resp
			}(replica)
		}

		go func() {
			wg.Wait()
			close(resultsChan) // close only after all the goroutines are done
			fmt.Printf("closing resultsChan\n")
		}()

		// Check for quorum
		// quorumValue, err := replication.ReceiveQuorum(ctx, resultsChan, len(replicas))
		// if err != nil {
		// 	return &pb.ReadResponse{}, err
		// }
		// fmt.Printf("quorumValue: %v\n", quorumValue)

		// return the value
		return &pb.ReadResponse{
			Date:     "2021-09-01T00:00:00Z",
			PageId:   req.PageId,
			Columns:  []string{"event", "componentId", "count"},
			Values:   []string{"click", "btn1", "1"}, // for now just retrieve a single row
			Name:     os.Getenv("NODE_NAME"),         // name of node which replied
			NodeType: "IS_NODE",
		}, nil
	}
}
