package replication

// import (
// 	"context"
// 	"errors"

// 	pb "github.com/uimagine-admin/tunadb/api"
// )

// func ReceiveQuorum(ctx context.Context, resultsChan chan *pb.ReadResponse, numReplicas int) (*pb.ReadResponse, error) {
// 	// check for quorum
// 	quorum := numReplicas/2 + 1
// 	responses := make(map[string]int)
// 	var quorumValue string
// 	success := 0

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return &pb.ReadResponse{}, errors.New("read quorum not reached due to timeout or cancellation")
// 		case resp, ok := <-resultsChan:
// 			if !ok {
// 				if success < quorum {
// 					return &pb.ReadResponse{}, errors.New("read quorum not reached")
// 				}
// 				return &pb.ReadResponse{
// 					Date:     resp.Date,
// 					PageId:   resp.PageId,
// 					Columns:  resp.Columns,
// 					Name:     resp.Name,
// 					NodeType: resp.NodeType,
// 					Values:   []string{quorumValue},
// 				}, nil
// 			}

// 			// extract value for quorum check
// 			val := resp.Values[0] // TODO change if we have more than one column
// 			responses[val]++
// 			success++

// 			if responses[val] >= quorum {
// 				quorumValue = val
// 				return &pb.ReadResponse{
// 					Date:     resp.Date,
// 					PageId:   resp.PageId,
// 					Columns:  resp.Columns,
// 					Name:     resp.Name,
// 					NodeType: resp.NodeType,
// 					Values:   []string{quorumValue},
// 				}, nil
// 			}
// 		}
// 	}
// }
