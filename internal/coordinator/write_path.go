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
	"strconv"
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
		err := db.HandleInsert(h.GetNode().ID, req, h.absolutePathSaveDir)
		if err != nil {
			// TODO: ERROR HANDLING - @jaytaykay
			log.Printf("error: %s\n", err)
		}
		tokenStr := strconv.FormatUint(req.HashKey, 10)
		columns := []string{"Date", "PageId", "Event", "ComponentId", "HashKey"}
		values := []string{req.Date, req.PageId, req.Event, req.ComponentId, tokenStr}
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
		// Add the token to the request, so the coordinator and the nodes can easily look up values 
		// during redistribution
		tokenStr := strconv.FormatUint(token, 10)
		req.HashKey = token
		if len(replicas) == 0 {
			return &pb.WriteResponse{}, errors.New("no available node for key")
		}

		resultsChan := make(chan *pb.WriteResponse, len(replicas))
		wg := sync.WaitGroup{}

		// for each replica: i'll send an internal write request
		for _, replica := range replicas {
			if replica.Name == os.Getenv("NODE_NAME") {
				err := db.HandleInsert(h.GetNode().ID, req, h.absolutePathSaveDir)
				if err != nil {
					log.Printf("error: %s\n", err)
				}
				columns := []string{"Date", "PageId", "Event", "ComponentId", "HashKey"}
				values := []string{req.Date, req.PageId, req.Event, req.ComponentId,tokenStr }
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

func (h *CoordinatorHandler) BulkWrite(ctx context.Context, bulkReq *pb.BulkWriteRequest) (*pb.BulkWriteResponse, error) {
	//if incoming is from node:
	if bulkReq.NodeType == "IS_NODE" {
		for _,rowData :=range  bulkReq.Data{
			req:=&pb.WriteRequest{
				Date:        rowData.Data["Date"],
				PageId:      rowData.Data["PageId"],
				Event:       rowData.Data["Event"],
				ComponentId: rowData.Data["ComponentId"],
				Name:        os.Getenv("NODE_NAME"),
				NodeType:    "IS_NODE",
				HashKey:    bulkReq.HashKey,}

			err := db.HandleInsert(h.GetNode().ID, req, h.absolutePathSaveDir)
			if err != nil {
				
				log.Printf("bulk write error: %s\n", err)
				return &pb.BulkWriteResponse{}, nil
			}
			tokenStr := strconv.FormatUint(req.HashKey, 10)
			columns := []string{"Date", "PageId", "Event", "ComponentId", "HashKey"}
			values := []string{req.Date, req.PageId, req.Event, req.ComponentId, tokenStr}
			log.Printf("writing rows and cols to db %s , %s\n", values, columns)

		}
		
		

		return &pb.BulkWriteResponse{
			Ack:      true,
			Name:     os.Getenv("NODE_NAME"),
			NodeType: "IS_NODE",
		}, nil

	}else{
		log.Printf("client bulk write not supported\n")
		return &pb.BulkWriteResponse{}, errors.New("client bulk write not supported")
	}
}