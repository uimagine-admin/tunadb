package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/communication"
	"github.com/uimagine-admin/tunadb/internal/db"
	"github.com/uimagine-admin/tunadb/internal/replication"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// Handles client Read request. fetches the value for a given key with quorum-based consistency.
func (h *CoordinatorHandler) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	if req.NodeType == "IS_NODE" {
		log.Printf(CoordinatorReadRequest + "[%s] Received read request from %s , PageId: %s ,Date: %s, columns: %s" + Reset, h.currentNode.ID, req.Name, req.PageId, req.Date, req.Columns)
		var rowResults []*pb.RowData
		rows, err := db.HandleRead(h.GetNode().ID, req, h.absolutePathSaveDir)
		if err != nil {
			log.Printf(GeneralError + "[%s] Error reading row: %s , error: %s\n" + Reset, h.currentNode.ID, rows, err)
			return &pb.ReadResponse{}, err
		}
		for _, row := range rows {
			rowResults = append(rowResults, &pb.RowData{
				Data: map[string]string{
					"Date":        row.Timestamp,
					"PageId":      row.PageId,
					"Event":       row.Event,
					"ComponentId": row.ComponentId,
				},
			})
		}
		columns := []string{"Date", "PageId", "Event", "ComponentId"}

		return &pb.ReadResponse{
			Date:    req.Date,
			PageId:  req.PageId,
			Columns: columns,
			Rows:    rowResults,
		}, nil
	} else {
		log.Printf(ClientReadRequest + "[%s] Received read request from [Client]: PageId: %s ,Date: %s, columns: %s" + Reset, h.currentNode.ID, req.PageId, req.Date, req.Columns)
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
				rows, err := db.HandleRead(h.GetNode().ID, req, h.absolutePathSaveDir)
				if err != nil {
					log.Printf("[%s] Error reading row: %s , error: %s\n", h.currentNode.ID, rows, err)
					return &pb.ReadResponse{}, err
				}
				for _, row := range rows {
					rowResults = append(rowResults, &pb.RowData{
						Data: map[string]string{
							"Date":        row.Timestamp,
							"PageId":      row.PageId,
							"Event":       row.Event,
							"ComponentId": row.ComponentId,
						},
					})
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

					log.Printf(CoordinatorReadRequest + "[%s] ReadPath: current node sent to results chan\n" + Reset, h.currentNode.ID)
				} else {
					log.Printf(CoordinatorReadRequest + "[%s] ReadPath: no rows found in %v \n" + Reset, h.currentNode.ID, replica.ID)
				}
				wg.Done()
				continue
			}

			go func(replica *types.Node) {
				defer wg.Done()

				// TODO replica.Name should be replaced with replica.Address
				address := fmt.Sprintf("%s:%d", replica.IPAddress, replica.Port)

				ctx_read, _ := context.WithCancel(context.Background())
				//send the read request with the key
				resp, err := communication.SendRead(&ctx_read, address, &pb.ReadRequest{
					Date:     req.Date,
					PageId:   req.PageId,
					Name:     os.Getenv("NODE_NAME"),
					Columns:  []string{"event", "componentId", "count"},
					NodeType: "IS_NODE",
				})

				if err != nil {
					log.Printf(GeneralError + "[%s] Error reading from %s: %v\n" + Reset, h.currentNode.ID, replica.ID, err)
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

				log.Printf("[%s] ReadPath: sent to results chan %s: %v\n", h.currentNode.ID, replica.ID, resp)
			}(replica)
		}

		go func() {
			wg.Wait()
			close(resultsChan) // close only after all the goroutines are done
		}()

		// Block on responses to resultsChan to Check for quorum
		quorumValue, err,faulty_nodes := replication.ReceiveReadQuorum(ctx, resultsChan, len(replicas))

		if err != nil {
			return &pb.ReadResponse{}, err
		}
		log.Printf("quorumValue: %v\n", quorumValue)
		
		if faulty_nodes !=nil{
			log.Printf(GeneralError + "[%s] Faulty nodes: %v\n" + Reset, h.currentNode.ID, faulty_nodes)
		}		
		// start of read repair
		if faulty_nodes != nil {
			cfmChan := make(chan *pb.BulkWriteResponse, len(faulty_nodes))
			wg_repair := sync.WaitGroup{}
			for _, node_name := range faulty_nodes {
				for _, replica := range replicas{
					if replica.Name==node_name{
						wg_repair.Add(1)
						log.Printf(CoordinatorWriteRequest + "[%s] Send read repair to node %v\n" + Reset, h.currentNode.ID, node_name)
						//start of send read repair
						go func(replica *types.Node) {
							defer wg_repair.Done()
			
							address := fmt.Sprintf("%s:%d", replica.IPAddress, replica.Port)
			
							ctx_write, _ := context.WithCancel(context.Background())
							
		
							ring := h.GetRing()
							// Add the token to the request, so the coordinator and the nodes can easily look up values 
							// during redistribution
							token, _ := ring.GetRecordsReplicas(req.PageId)

							resp, err := communication.SendBulkWrite(&ctx_write, address, &pb.BulkWriteRequest{
								Name:        os.Getenv("NODE_NAME"),
								Data:  quorumValue.Rows,//[]*pb.RowData{}
								NodeType:    "IS_NODE",
								HashKey:    token,})

							cfmChan<-resp
							if err != nil {
								log.Printf(GeneralError + "[%s] Error writing to %s: %v\n" + Reset, h.currentNode.ID, replica.ID, err)
								return
							}
						}(replica)
					}
				}
		}
		go func() {
			wg_repair.Wait()
			close(cfmChan) // close only after all the goroutines are done
		}()
		//blocking call to check if received all repair cfm messages
		replication.ReceiveBulkWriteConfirm(ctx, cfmChan, len(faulty_nodes))
		
	}
	return quorumValue, nil
}
}
