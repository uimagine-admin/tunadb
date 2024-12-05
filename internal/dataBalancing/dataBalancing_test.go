package dataBalancing

// import (
// 	"fmt"
// 	"log"
// 	"net"
// 	"strconv"
// 	"testing"

// 	pb "github.com/uimagine-admin/tunadb/api"
// 	"github.com/uimagine-admin/tunadb/internal/db"
// 	"github.com/uimagine-admin/tunadb/internal/gossip"
// 	"github.com/uimagine-admin/tunadb/internal/ring"
// 	"github.com/uimagine-admin/tunadb/internal/types"
// 	"google.golang.org/grpc"
// )

// type server struct {
// 	pb.UnimplementedCassandraServiceServer
// 	GossipHandler *gossip.GossipHandler
// 	nodeRingView   *ring.ConsistentHashingRing
// }

// // handle incoming gossip request
// func StartServer(ringView *ring.ConsistentHashingRing, gossipHandler *gossip.GossipHandler, portInternal string) {
// 	log.Printf("Listening on %s", fmt.Sprintf(":%s", portInternal))
// 	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", portInternal))
// 	if err != nil {
// 		log.Fatalf("Failed to listen: %v", err)
// 	}

// 	grpcServer := grpc.NewServer()
// 	pb.RegisterCassandraServiceServer(grpcServer, &server{
// 		GossipHandler: gossipHandler,
// 		nodeRingView:   ringView,
// 	})

// 	log.Printf("gRPC server is running on port %s", portInternal)
// 	if err := grpcServer.Serve(lis); err != nil {
// 		log.Fatalf("Failed to serve: %v", err)
// 	}
// }

// func TestNewNodeRebBalancing(t *testing.T) {
// 	nodeA := types.Node{ID: "3", Name: "NodeA", IPAddress: "127.0.0.1", Port: 50003}
// 	nodeB := types.Node{ID: "4", Name: "NodeB", IPAddress: "127.0.0.1", Port: 50004}

// 	ringViewA := ring.CreateConsistentHashingRing(1,1)
// 	ringViewA.AddNode(nodeA)

// 	ringViewB := ring.CreateConsistentHashingRing(1,1)
// 	ringViewB.AddNode(nodeB)
// 	ringViewB.AddNode(nodeA)

// 	go StartServer(ringViewA, nil, "50003")
// 	go StartServer(ringViewB, nil, "50004")

// 	// initializes just node A with some data records
// 	nodeAData := []map[string]string{
// 		{
// 			"pageID": "1",
// 			"element": "button2",
// 			"timestamp": "2023-11-26T12:01:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "1",
// 		  },
// 		  {
// 			"pageID": "2",
// 			"element": "link1",
// 			"timestamp": "2023-11-26T12:02:00Z",
// 			"event": "click",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "1",
// 		  },
// 		  {
// 			"pageID": "3",
// 			"element": "button1",
// 			"timestamp": "2023-11-26T12:03:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "1",
// 		  },
// 		  {
// 			"pageID": "4",
// 			"element": "button1",
// 			"timestamp": "2023-11-26T12:04:00Z",
// 			"event": "click",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "1",
// 		  },
// 		  {
// 			"pageID": "5",
// 			"element": "button2",
// 			"timestamp": "2023-11-26T12:05:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "1",
// 		  },
// 		  {
// 			"pageID": "5",
// 			"element": "button3",
// 			"timestamp": "2023-11-26T12:06:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "2",
// 		  },
// 		  {
// 			"pageID": "5",
// 			"element": "button1",
// 			"timestamp": "2023-11-26T12:07:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "3",
// 		  },
// 		  {
// 			"pageID": "5",
// 			"element": "button1",
// 			"timestamp": "2023-11-26T12:08:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "4",
// 		  },
// 		  {
// 			"pageID": "5",
// 			"element": "button1",
// 			"timestamp": "2023-11-26T12:09:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "5",
// 		  },
// 		  {
// 			"pageID": "5",
// 			"element": "button1",
// 			"timestamp": "2023-11-26T12:10:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "6",
// 		  },
// 		  {
// 			"pageID": "5",
// 			"element": "button1",
// 			"timestamp": "2023-11-26T12:11:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "7",
// 		  },
// 		  {
// 			"pageID": "5",
// 			"element": "button1",
// 			"timestamp": "2023-11-26T12:12:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "8",
// 		  },
// 		  {
// 			"pageID": "5",
// 			"element": "button1",
// 			"timestamp": "2023-11-26T12:13:00Z",
// 			"event": "hover",
// 			"updated_at": "",
// 			"created_at": "",
// 			"hashKey": "9",
// 		  },
// 	}

// 	for _, row := range nodeAData {
// 		hashKey, errParse := strconv.ParseUint(row["hashKey"], 10, 64)
// 		if errParse != nil {
// 			t.Errorf("Failed to parse hash key: %v", errParse)
// 		}
// 		err := db.HandleInsert(nodeA.ID, &pb.WriteRequest{
// 			Date:         row["timestamp"],
// 			PageId:       row["pageID"],
// 			Event:        row["event"],
// 			ComponentId:  row["element"],
// 			HashKey:      hashKey,
// 			NodeType:     "IS_NODE",
// 			Name:         "NodeA",
// 		})
// 		if err != nil {
// 			t.Errorf("Failed to insert data: %v", err)
// 		}
// 	}

// 	ringViewB.AddNode(nodeA)
// 	oldTokenRanges := ringViewA.AddNode(nodeB)

// 	dh := DistributionHandler{
// 		Ring: ringViewA,
// 		CurrentNodeID: nodeA.ID,
// 	}

// 	err := dh.TriggerDataRedistribution(oldTokenRanges)

// 	if err != nil {
// 		t.Errorf("Failed to trigger data redistribution: %v", err)
// 	}

// 	select{

// 	}
// }




