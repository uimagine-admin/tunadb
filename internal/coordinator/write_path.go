//write logic would be here:
//if coordinator : hash -> send to other nodes , await reply , if crash detected->repair
//if not coordinator : wirte to commitlog->memtable->check if memtable>capacity ->SStable

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// Write writes a value for a given key with quorum-based consistency.
// func (h *CoordinatorHandler)
func (h *CoordinatorHandler) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	ring := h.GetRing()
	replicas := ring.GetNodes(req.PageId)
	if len(replicas) == 0 {
		return &pb.WriteResponse{}, errors.New("no available node for key")
	}

	resultsChan := make(chan pb.WriteResponse, len(replicas))
	defer close(resultsChan)
	wg := sync.WaitGroup{}

	// for each replica: send write
	for _, replica := range replicas {
		wg.Add(1)
		go func(replica types.Node) {
			defer wg.Done()

			address := fmt.Sprintf("%s:%d", replica.Name, replica.Port)

			// TODO replace with updated sendWrite,
			//send the write request with the key and value
			// resp, err := sendWrite(ctx, address, &pb.WriteRequest{
			// 	PageId:  key,
			// 	Name:    h.GetNode().Name,
			// 	Columns: []string{"value"},
			// })
			// mock the sendWrite
			var err error = nil
			resp := pb.WriteResponse{
				Ack: true,
			}

			if err != nil {
				fmt.Printf("error writing to %s: %v\n", address, err)
				return
			}

			// get the reply
			resultsChan <- resp
		}(replica)
	}

	// get the reply
	go func() {
		wg.Wait()
	}()

	return &pb.WriteResponse{
		Ack:      true,
		Name:     h.currentNode.Name,
		NodeType: "IS_NODE",
	}, nil
}
