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
	var receivedResponses []*pb.ReadResponse = make([]*pb.ReadResponse, 0)
	var mostRecentDate time.Time

	for {
		select {
		case <-ctx.Done():
			return nil, errors.New("read quorum not reached due to timeout or cancellation")
		case resp, ok := <-resultsChan:
			if ok {
				log.Printf("quorum: received response from %v with date %v \n", resp.Name, resp.Date)
				success++
				// store the response in the array of responses
				receivedResponses = append(receivedResponses, resp)

				respDate, err := time.Parse(time.RFC3339, resp.Date)
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

					resp, faultyNodes, err := MergeReadResponses(receivedResponses)
					if len(faultyNodes) > 0 {
						log.Printf("faulty nodes found: %v \n", faultyNodes)
					}
					if err != nil {
						return nil, err
					}
					return resp, nil
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

				resp, faultyNodes, err := MergeReadResponses(receivedResponses)
				log.Printf("faulty nodes found: %v \n", faultyNodes)
				if err != nil {
					return nil, err
				}
				return resp, nil
			}
		}
	}
}

// helper function to merge the results from multiple replicas and return the final response
func MergeReadResponses(responses []*pb.ReadResponse) (*pb.ReadResponse, []string, error) {
	log.Printf("merging %d responses \n", len(responses))

	if len(responses) == 0 {
		log.Printf("no responses to merge \n")
		return nil, nil, errors.New("no responses to merge")
	}

	mergedResponse := &pb.ReadResponse{
		Date:     responses[0].Date,
		PageId:   responses[0].PageId,
		Columns:  responses[0].Columns,
		Name:     responses[0].Name,
		NodeType: responses[0].NodeType,
		Rows:     []*pb.RowData{},
	}

	// keep a map to store unique rows based on the primary key
	uniqueRows := make(map[string]*pb.RowData)
	// map of node names to set of primary keys they have
	nodeKeys := make(map[string]map[string]struct{})
	// set of all primary keys
	allKeys := make(map[string]struct{})

	for _, resp := range responses {
		if resp == nil {
			continue
		}

		nodeName := resp.Name
		if _, exists := nodeKeys[nodeName]; !exists {
			nodeKeys[nodeName] = make(map[string]struct{})
		}

		for _, row := range resp.Rows {
			// generate primary key
			pageId := row.Data["PageId"]
			date := row.Data["Date"]
			primaryKey := fmt.Sprintf("%s-%s", pageId, date)

			// add row to rowmap if it doesn't exist
			if _, exists := uniqueRows[primaryKey]; !exists {
				uniqueRows[primaryKey] = row
			}

			// record that this node has this primary key
			nodeKeys[nodeName][primaryKey] = struct{}{}

			// add to set of all primary keys
			allKeys[primaryKey] = struct{}{}
		}
	}

	// build merged response
	for _, row := range uniqueRows {
		mergedResponse.Rows = append(mergedResponse.Rows, row)
	}

	nodesMissingRows := make([]string, 0)
	for nodeName, keys := range nodeKeys {
		for primaryKey := range allKeys {
			if _, exists := keys[primaryKey]; !exists {
				nodesMissingRows = append(nodesMissingRows, nodeName)
				break
			}
		}
	}

	nodesMissingRows = removeDuplicates(nodesMissingRows)

	return mergedResponse, nodesMissingRows, nil
}

// Helper function to remove duplicates from a slice of strings
func removeDuplicates(input []string) []string {
	keys := make(map[string]struct{})
	list := []string{}
	for _, entry := range input {
		if _, value := keys[entry]; !value {
			keys[entry] = struct{}{}
			list = append(list, entry)
		}
	}
	return list
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
