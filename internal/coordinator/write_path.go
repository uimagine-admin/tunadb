//write logic would be here:
//if coordinator : hash -> send to other nodes , await reply , if crash detected->repair
//if not coordinator : wirte to commitlog->memtable->check if memtable>capacity ->SStable

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"os"
	"time"


	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/types"
	"github.com/uimagine-admin/tunadb/internal/communication"
)

// Write writes a value for a given key with quorum-based consistency.
// func (h *CoordinatorHandler)
func (h *CoordinatorHandler) Write(ctx *context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	//if incoming is from node:
	if req.NodeType=="IS_NODE"{

		//TODO: write to database
		columns := []string{"Date", "PageId", "Event", "ComponentId"}
		values := []string{req.Date, req.PageId, req.Event, req.ComponentId}
		fmt.Printf("writing rows and cols to db %s , %s\n", values,columns)
		//write to commitlog->memtable-->SStable

		return &pb.WriteResponse{
			Ack:   true,
			Name: os.Getenv("NODE_NAME"),
			NodeType:"IS_NODE",
		}, nil

	}else{
		//if incoming is from client:
		ring := h.GetRing()
		replicas := ring.GetNodes(req.PageId)
		if len(replicas) == 0 {
			return &pb.WriteResponse{}, errors.New("no available node for key")
		}

		resultsChan := make(chan *pb.WriteResponse, len(replicas))
		wg := sync.WaitGroup{}

		// for each replica: i'll send an internal write request
		for _, replica := range replicas {
			if replica.Name == os.Getenv("NODE_NAME"){
				//TODO: write to database
				columns := []string{"Date", "PageId", "Event", "ComponentId"}
				values := []string{req.Date, req.PageId, req.Event, req.ComponentId}
				fmt.Printf("writing rows and cols to db %s , %s\n", values,columns)
				continue
			} //so it doesnt send to itself
			wg.Add(1)
			go func(replica types.Node) {
				defer wg.Done()

				address := fmt.Sprintf("%s:%d", replica.Name, replica.Port)
				
				ctx_write, _ := context.WithTimeout(context.Background(), time.Second)
				
				resp, err := communication.SendWrite(&ctx_write, address, &pb.WriteRequest{
					Date:req.Date,
					PageId:req.PageId,
					Event: req.Event,
					ComponentId:req.ComponentId,
					Name: os.Getenv("NODE_NAME"),
					NodeType:"IS_NODE",})

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

		// After getting all acknowledgements, return the response
		// Future implementation : check who did not send acknowledgements and repair fault

		return &pb.WriteResponse{
			Ack:   true,
			Name: os.Getenv("NODE_NAME"),
			NodeType:"IS_NODE",
		}, nil

	}
}
