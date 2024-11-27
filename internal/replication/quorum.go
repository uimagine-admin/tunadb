package replication

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
)

func ReceiveReadQuorum(ctx context.Context, resultsChan chan *pb.ReadResponse, numReplicas int) (*pb.ReadResponse, error) {
	// check for quorum
	quorum := numReplicas/2 + 1
	success := 0
	var lastResponse *pb.ReadResponse
	var mostRecentDate time.Time

	for {
		select {
		case <-ctx.Done():
			return nil, errors.New("read quorum not reached due to timeout or cancellation")
		case resp, ok := <-resultsChan:
			if ok {
				log.Printf("quorum: received response from %v with date %v \n", resp.Name, resp.Date)
				success++

				respDate, err := time.Parse("2006-01-02", resp.Date)
				if err != nil {
					log.Printf("Error parsing date from %s: %v", resp.Name, err)
					continue
				}

				if lastResponse == nil || respDate.After(mostRecentDate) {
					lastResponse = resp
					mostRecentDate = respDate
				}

				if success >= quorum {
					log.Printf("quorum: quorum reached with %d responses \n", success)
					return lastResponse, nil
				}
			} else {
				// if channel is closed since all replicas have sent their results, return error if quorum not reached
				if success < quorum {
					return nil, errors.New("read quorum not reached")
				}

				log.Printf("quorum: quorum reached with %d responses \n", success)
				if lastResponse == nil {
					return nil, errors.New("no response received")
				}
				return lastResponse, nil
			}
		}
	}
}

func ReceiveWriteQuorum(ctx context.Context, resultsChan chan *pb.WriteResponse, numReplicas int) (*pb.WriteResponse, error) {
	// check for quorum
	quorum := numReplicas/2 + 1
	success := 0
	numReplies := 0

	for {
		select {
		case <-ctx.Done():
			return nil, errors.New("write quorum not reached due to timeout or cancellation")
		case resp, ok := <-resultsChan:
			if ok {
				numReplies++
				log.Printf("quorum: received %v response from %v \n", resp.Ack, resp.Name)
				if resp.Ack {
					success++
				}

				if success >= quorum {
					log.Printf("quorum: quorum reached with %d Acks out of %d responses \n", success, numReplies)
					return &pb.WriteResponse{
						Ack:      true,
						Name:     os.Getenv("NODE_NAME"),
						NodeType: "IS_NODE",
					}, nil
				}
			} else {
				// if channel is closed since all replicas have sent their results, return error if quorum not reached
				if success < quorum {
					return nil, errors.New(fmt.Sprintf("write quorum not reached: Only got Acks from %d out of %d replicas", numReplies, numReplicas))
				}

				log.Printf("quorum: quorum reached with %d Acks out of %d responses \n", success, numReplies)

				return &pb.WriteResponse{
					Ack:      true,
					Name:     os.Getenv("NODE_NAME"),
					NodeType: "IS_NODE",
				}, nil
			}
		}
	}
}
