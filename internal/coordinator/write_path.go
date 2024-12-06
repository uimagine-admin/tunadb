//write logic would be here:
//if coordinator : hash -> send to other nodes , await reply , if crash detected->repair
//if not coordinator : wirte to commitlog->memtable->check if memtable>capacity ->SStable

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

// Write writes a value for a given key with quorum-based consistency.
// func (h *CoordinatorHandler)
func (h *CoordinatorHandler) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	//if incoming is from node:
	if req.NodeType == "IS_NODE" {
		err := db.HandleInsert(h.GetNode().ID, req)
		if err != nil {
			// TODO: ERROR HANDLING - @jaytaykay
			log.Printf("error: %s\n", err)
		}
		columns := []string{"Date", "PageId", "Event", "ComponentId"}
		values := []string{req.Date, req.PageId, req.Event, req.ComponentId}
		log.Printf("writing rows and cols to db %s , %s\n", values, columns)
		//write to commitlog->memtable-->SStable

		return &pb.WriteResponse{
			Ack:      true,
			Name:     os.Getenv("NODE_NAME"),
			NodeType: "IS_NODE",
		}, nil

	} else {
		//if incoming is from client:
		ring := h.GetRing()
		token, replicas := ring.GetRecordsReplicas(req.PageId)
		if len(replicas) == 0 {
			return &pb.WriteResponse{}, errors.New("no available node for key")
		}

		resultsChan := make(chan *pb.WriteResponse, len(replicas))
		wg := sync.WaitGroup{}

		// for each replica: i'll send an internal write request
		for _, replica := range replicas {
			if replica.Name == os.Getenv("NODE_NAME") {
				err := db.HandleInsert(h.GetNode().ID, req)
				if err != nil {
					log.Printf("error: %s\n", err)
				}
				columns := []string{"Date", "PageId", "Event", "ComponentId"}
				values := []string{req.Date, req.PageId, req.Event, req.ComponentId}
				log.Printf("writing rows and cols to db %s , %s\n", values, columns)

				resultsChan <- &pb.WriteResponse{
					Ack:      true,
					Name:     os.Getenv("NODE_NAME"),
					NodeType: "IS_NODE",
				}

				log.Printf("WritePath: current node sent ack to results chan\n")

				continue
			} //so it doesnt send to itself
			wg.Add(1)
			go func(replica *types.Node) {
				defer wg.Done()

				address := fmt.Sprintf("%s:%d", replica.Name, replica.Port)

				ctx_write, _ := context.WithTimeout(context.Background(), time.Second)

				resp, err := communication.SendWrite(&ctx_write, address, &pb.WriteRequest{
					Date:        req.Date,
					PageId:      req.PageId,
					Event:       req.Event,
					ComponentId: req.ComponentId,
					Name:        os.Getenv("NODE_NAME"),
					NodeType:    "IS_NODE",
					HashKey:    token,})

				if err != nil {
					log.Printf("error reading from %s: %v\n", address, err)
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

		// After getting all acknowledgements, return the response
		// Future implementation : check who did not send acknowledgements and repair fault
		quorumValue, err := replication.ReceiveWriteQuorum(ctx, resultsChan, len(replicas))
		if err != nil {
			return &pb.WriteResponse{Ack: false, Name: os.Getenv("NODE_NAME"), NodeType: "IS_NODE"}, err
		}
		log.Printf("quorum: quorum reached with %v responses \n", quorumValue)
		return &pb.WriteResponse{Ack: true, Name: os.Getenv("NODE_NAME"), NodeType: "IS_NODE"}, nil

		// select {
		// case <-resultsChan:
		// 	return &pb.WriteResponse{Ack: true,
		// 		Name:     os.Getenv("NODE_NAME"),
		// 		NodeType: "IS_NODE"}, nil
		// case <-time.After(5 * time.Second):
		// 	return &pb.WriteResponse{Ack: false, Name: os.Getenv("NODE_NAME"), NodeType: "IS_NODE"}, errors.New("timeout")
		// }

	}
}
