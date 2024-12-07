package dataBalancing

import (
	"context"
	"io"
	"log"
	"strconv"
	"sync"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/db"
	"github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
	"google.golang.org/grpc"
)

// DistributionHandler manages redistribution during ring updates
type DistributionHandler struct {
	Ring *ring.ConsistentHashingRing
	mu   sync.Mutex
	CurrentNode *types.Node
}

// SyncData redistributes data based on updated ring state
func (dh *DistributionHandler) HandleDataSync(ctx context.Context, req *pb.SyncDataRequest) (*pb.SyncDataResponse, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	for _, row := range req.Data {
		log.Printf("Syncing data: %+v\n", row.Data)
		// Insert data into the local node's storage
		hashedKeyString , keyFound := row.Data["hashKey"]
		if !keyFound {
			log.Printf("Hash key not found in data record: %v", row.Data)
			return nil, nil
		}

		hashedKey, errorConvert := strconv.ParseUint(hashedKeyString, 10, 64)

		if errorConvert != nil {
			log.Printf("If false it means the record did not exist. \nOtherwise failed to convert hashKey while inserting data record: %v.", errorConvert)
			return nil, nil
		}

		err := db.HandleInsert(dh.CurrentNode.ID, &pb.WriteRequest{
			Date:         row.Data["timestamp"],
			PageId:       row.Data["page_id"],
			Event:        row.Data["event"],
			ComponentId:  row.Data["component_id"],
			HashKey:      hashedKey,
			NodeType:     "IS_NODE",
			Name:         req.Sender,
		})

		if err != nil {
			log.Printf("Failed to insert data into JSON: %v", err)
			return nil, err
		}
	}

	return &pb.SyncDataResponse{
		Status:  "success",
		Message: "Data synced successfully",
	}, nil
}

// TriggerDataRedistribution triggers redistribution after ring state change
func (dh *DistributionHandler) TriggerDataRedistribution(oldTokenRanges map[string][]ring.TokenRange) error {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	log.Printf("[%s] Triggering data redistribution...", dh.CurrentNode.ID)

	for _, replica := range dh.Ring.GetRingMembers() {
		tokenRanges := dh.Ring.GetTokenRangeForNode(replica.ID)
		for _, tokenRange := range tokenRanges {
			// If the token range is not present in the old ring state, send data to the new owner
			found := false
			for _, oldTokenRange := range oldTokenRanges[replica.ID] {
				if tokenRange.Start == oldTokenRange.Start && tokenRange.End == oldTokenRange.End {
					found = true
					break
				}
			}
			if !found && replica.ID != dh.CurrentNode.ID {
				go dh.sendDataForTokenRange(replica, tokenRange)
			}
		}
	}

	return nil
}

// sendDataForTokenRange streams data from the current owner to the new owner
func (dh *DistributionHandler) sendDataForTokenRange(replica *types.Node, tokenRange ring.TokenRange) {
	// log.Printf("[%s] Sending data for TokenRange %v-%v to new owner...", dh.CurrentNode.ID ,tokenRange.Start, tokenRange.End)

	dataRows, err := db.HandleRecordsFetchByHashKey(dh.CurrentNode.ID, tokenRange)
	if err != nil {
		// log.Printf("[%s] Failed to fetch data for TokenRange %v-%v: %v", dh.CurrentNode.ID, tokenRange.Start, tokenRange.End, err)
		return 
	}

	// Establish gRPC connection to new owner
	if replica == nil {
		// log.Printf("[%s] No new owner found for TokenRange %v-%v", dh.CurrentNode.ID, tokenRange.Start, tokenRange.End)
		return
	}

	replicasAddress := replica.IPAddress + ":" + strconv.FormatUint(replica.Port, 10)

	conn, err := grpc.Dial(replicasAddress, grpc.WithInsecure()) // Replace with secure connection in production
	if err != nil {
		// log.Printf("[%s] Failed to connect to new owner %s: %v", dh.CurrentNode.ID, replica.String(), err)
		return
	}
	defer conn.Close()

	client := pb.NewCassandraServiceClient(conn)

	// Create gRPC stream
	stream, err := client.SyncData(context.Background())
	if err != nil {
		// log.Printf("[%s] Failed to open stream to new owner: %v", dh.CurrentNode.ID, err)
		return
	}

	// TODO: batch data rows and send in chunks
	req := &pb.SyncDataRequest{
		Sender: dh.CurrentNode.ID,
		Data:   dataRows,
	}
	if err := stream.Send(req); err != nil {
		// log.Printf("[%s] Failed to send data row: %v", dh.CurrentNode.ID, err)
		return
	}

	// Close the send direction of the stream
	if err := stream.CloseSend(); err != nil {
		// log.Printf("[%s] Error closing stream: %v", dh.CurrentNode.ID, err)
		return
	}

	// Handle stream responses
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[%s] Error receiving stream response: %v", dh.CurrentNode.ID, err)
			return
		}
		log.Printf("[%s] Received response: %s", dh.CurrentNode.ID, resp.Message)
	}
}


