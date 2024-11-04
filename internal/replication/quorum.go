package replication

import (
	"context"
	"errors"

	pb "github.com/uimagine-admin/tunadb/api"
)

func ReceiveQuorum(ctx context.Context, resultsChan chan pb.ReadResponse, numReplicas int) (string, error) {
	// check for quorum
	quorum := numReplicas/2 + 1
	responses := make(map[string]int)
	var quorumValue string
	success := 0

	for {
		select {
		case <-ctx.Done():
			return "", errors.New("read quorum not reached due to timeout or cancellation")
		case resp, ok := <-resultsChan:
			if !ok {
				if success < quorum {
					return "", errors.New("read quorum not reached")
				}
				return quorumValue, nil
			}

			// extract value for quorum check
			val := resp.Values[0] // TODO change if we have more than one column
			responses[val]++

			if responses[val] >= quorum {
				quorumValue = val
				return quorumValue, nil
			}
		}
	}
}
