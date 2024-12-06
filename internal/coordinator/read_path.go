package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/communication"
	"github.com/uimagine-admin/tunadb/internal/db"
	"github.com/uimagine-admin/tunadb/internal/replication"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// Handles client Read request. fetches the value for a given key with quorum-based consistency.
func (h *CoordinatorHandler) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	if req.NodeType == "IS_NODE" {
		var rowResults []*pb.RowData
		rows, err := db.HandleRead(h.GetNode().ID, req)
		if err != nil {
			log.Printf("Error reading row: %s , error: %s\n", rows, err)
			return &pb.ReadResponse{}, err
		}
		for i, row := range rows {
			rowResults = append(rowResults, &pb.RowData{
				Data: map[string]string{
					"Date":        row.Timestamp,
					"PageId":      row.PageId,
					"Event":       row.Event,
					"ComponentId": row.ComponentId,
				},
			})
			log.Printf("reading row %d from db %s\n", i, row)
		}
		columns := []string{"Date", "PageId", "Event", "ComponentId"}

		return &pb.ReadResponse{
			Date:    req.Date,
			PageId:  req.PageId,
			Columns: columns,
			Rows:    rowResults,
		}, nil
	} else {
		ring := h.GetRing()
		_, replicas := ring.GetRecordsReplicas(req.PageId)

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
				var rowResults []*pb.RowData
				rows, err := db.HandleRead(h.GetNode().ID, req)
				if err != nil {
					log.Printf("Error reading row: %s , error: %s\n", rows, err)
					return &pb.ReadResponse{}, err
				}
				for i, row := range rows {
					rowResults = append(rowResults, &pb.RowData{
						Data: map[string]string{
							"Date":        row.Timestamp,
							"PageId":      row.PageId,
							"Event":       row.Event,
							"ComponentId": row.ComponentId,
						},
					})
					log.Printf("reading row %d from db %s\n", i, row)
				}

				if len(rowResults) > 0 {
					// send to the results channel
					resultsChan <- &pb.ReadResponse{
						Date:    req.Date,
						PageId:  req.PageId,
						Columns: []string{"Date", "PageId", "Event", "ComponentId"},
						Rows:    rowResults,
						Name:    os.Getenv("NODE_NAME"),
					}

					log.Printf("ReadPath: current node sent to results chan\n")
				} else {
					log.Printf("ReadPath: no rows found in %v\n", os.Getenv("NODE_NAME"))
				}
				wg.Done()
				continue
			}

			go func(replica *types.Node) {
				defer wg.Done()

				// TODO replica.Name should be replaced with replica.Address
				address := fmt.Sprintf("%s:%d", replica.Name, replica.Port)

				ctx_read, _ := context.WithTimeout(context.Background(), time.Second)
				//send the read request with the key
				resp, err := communication.SendRead(&ctx_read, address, &pb.ReadRequest{
					Date:     req.Date,
					PageId:   req.PageId,
					Name:     os.Getenv("NODE_NAME"),
					Columns:  []string{"event", "componentId", "count"},
					NodeType: "IS_NODE",
				})

				if err != nil {
					log.Printf("error reading from %s: %v\n", address, err)
					return
				}

				// get the reply
				resultsChan <- &pb.ReadResponse{
					Name:    replica.Name,
					Date:    resp.Date,
					PageId:  resp.PageId,
					Columns: resp.Columns,
					Rows:    resp.Rows,
				}

				log.Printf("ReadPath: sent to results chan %s: %v\n", address, resp)
			}(replica)
		}

		go func() {
			wg.Wait()
			close(resultsChan) // close only after all the goroutines are done
			log.Printf("closing resultsChan\n")
		}()

		// Block on responses to resultsChan to Check for quorum
		quorumValue, err := replication.ReceiveReadQuorum(ctx, resultsChan, len(replicas))
		if err != nil {
			return &pb.ReadResponse{}, err
		}
		log.Printf("quorumValue: %v\n", quorumValue)

		return quorumValue, nil
	}
}
