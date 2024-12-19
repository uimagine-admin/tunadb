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
		log.Printf(CoordinatorWriteRequest + "[%s] WritePath: received write request from %s , PageId: %s ,Date: %s, Event: %s, ComponentId: %s\n" + Reset, h.currentNode.ID, req.Name, req.PageId, req.Date, req.Event, req.ComponentId)
		err := db.HandleInsert(h.GetNode().ID, req, h.absolutePathSaveDir)
		if err != nil {
			// TODO: ERROR HANDLING - @jaytaykay
			log.Printf("[%s] Error: %s\n", h.currentNode.ID, err)
		}

		return &pb.WriteResponse{
			Ack:      true,
			Name:     os.Getenv("NODE_NAME"),
			NodeType: "IS_NODE",
		}, nil

	} else {
		log.Printf(ClientWriteRequest + "[%s] WritePath: received write request from [Client] , PageId: %s ,Date: %s, Event: %s, ComponentId: %s\n" + Reset, h.currentNode.ID, req.PageId, req.Date, req.Event, req.ComponentId)
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
				log.Printf(ClientWriteRequest + "[%s] Writing rows and cols to db %s , %s\n" + Reset, h.currentNode.ID, values, columns)

				resultsChan <- &pb.WriteResponse{
					Ack:      true,
					Name:     os.Getenv("NODE_NAME"),
					NodeType: "IS_NODE",
				}

				log.Printf(ClientWriteRequest + "[%s] WritePath: current node sent ack to results chan\n" + Reset, h.currentNode.ID)

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
					HashKey:    token,
				})

				if err != nil {
					log.Printf(GeneralError + "[%s] Error reading from %s: %v\n" + Reset, h.currentNode.ID, address, err)
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
				
				log.Printf(GeneralError + "[%s] Bulk write error: %s\n" + Reset, h.currentNode.ID, err)
				return &pb.BulkWriteResponse{}, nil
			}
		}
		
		

		return &pb.BulkWriteResponse{
			Ack:      true,
			Name:     os.Getenv("NODE_NAME"),
			NodeType: "IS_NODE",
		}, nil

	}else{
		log.Printf( GeneralDebug + "[%s] Client bulk write not supported\n" + Reset, h.currentNode.ID)
		return &pb.BulkWriteResponse{}, errors.New("client bulk write not supported")
	}
}