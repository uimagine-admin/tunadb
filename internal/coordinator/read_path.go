package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/communication"
	"github.com/uimagine-admin/tunadb/internal/replication"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// Handles client Read request. fetches the value for a given key with quorum-based consistency.
func (h *CoordinatorHandler) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
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
		go func(replica types.Node) {
			defer wg.Done()

			address := fmt.Sprintf("%s:%d", replica.Name, replica.Port)

			//send the read request with the key
			resp, err := communication.SendRead(&ctx, address, &pb.ReadRequest{
				Date:     time.Now().Format(time.RFC3339), // an ISO 8601 date string
				PageId:   req.PageId,
				Name:     h.GetNode().Name,
				Columns:  []string{"event", "componentId", "count"},
				NodeType: "IS_NODE",
			})

			// // mock the sendRead
			// var err error = nil
			// resp := pb.ReadResponse{Values: []string{"value" + replica.ID}}

			if err != nil {
				fmt.Printf("error reading from %s: %v\n", address, err)
				return
			}

			// get the reply
			resultsChan <- resp
		}(replica)
	}

	go func() {
		wg.Wait()
		close(resultsChan) // close only after all the goroutines are done
	}()

	// Check for quorum
	quorumValue, err := replication.ReceiveQuorum(ctx, resultsChan, len(replicas))
	if err != nil {
		return &pb.ReadResponse{}, err
	}

	// return the value
	return quorumValue, nil
}
