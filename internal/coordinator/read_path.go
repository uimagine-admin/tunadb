package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/replication"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// Read fetches the value for a given key with quorum-based consistency.
func (h *CoordinatorHandler) Read(ctx context.Context, key string) (string, error) {
	ring := h.GetRing()
	replicas := ring.GetNodes(key)
	if len(replicas) == 0 {
		return "", errors.New("no available node for key")
	}

	resultsChan := make(chan pb.ReadResponse, len(replicas))
	wg := sync.WaitGroup{}

	// for each replica: i'll send an internal read request
	for _, replica := range replicas {
		wg.Add(1)
		go func(replica types.Node) {
			defer wg.Done()

			address := fmt.Sprintf("%s:%d", replica.Name, replica.Port)

			// TODO replace with updated sendRead,
			//send the read request with the key
			// resp, err := sendRead(ctx, address, &pb.ReadRequest{
			// 	PageId:  key,
			// 	Name:    h.GetNode().Name,
			// 	Columns: []string{"value"},
			// })

			// mock the sendRead
			var err error = nil
			resp := pb.ReadResponse{Values: []string{"value" + replica.ID}}

			if err != nil {
				fmt.Printf("error reading from %s: %v\n", address, err)
				return
			}

			// get the reply
			resultsChan <- resp
		}(replica)
	}

	// get the reply
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Check for quorum
	quorumValue, err := replication.ReceiveQuorum(ctx, resultsChan, len(replicas))
	if err != nil {
		return "", err
	}

	// return the value
	return quorumValue, nil
}
