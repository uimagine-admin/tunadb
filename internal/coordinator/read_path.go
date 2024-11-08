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

		log.Printf("Received read request from %s , PageId: %s ,Date: %s, columns: %s\n", req.Name, req.PageId, req.Date, req.Columns)
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
				log.Printf("simulating read from db for pageID %s\n", req.PageId)
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
				continue
			}

			go func(replica types.Node) {
				defer wg.Done()

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

				log.Printf("Received read response from %s: %v\n", address, resp)

				// get the reply
				resultsChan <- resp
			}(replica)
		}

		go func() {
			wg.Wait()
			close(resultsChan) // close only after all the goroutines are done
			log.Printf("closing resultsChan\n")
		}()

		// Check for quorum
		// quorumValue, err := replication.ReceiveQuorum(ctx, resultsChan, len(replicas))
		// if err != nil {
		// 	return &pb.ReadResponse{}, err
		// }
		// log.Printf("quorumValue: %v\n", quorumValue)

		select {
		case quorumResponse := <-resultsChan:
			return quorumResponse, nil
		case <-time.After(5 * time.Second):
			return &pb.ReadResponse{}, errors.New("timeout")
		}
	}
}
