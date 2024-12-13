package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/coordinator"
	"github.com/uimagine-admin/tunadb/internal/dataBalancing"
	"github.com/uimagine-admin/tunadb/internal/gossip"
	"github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
	"github.com/uimagine-admin/tunadb/internal/utils"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedCassandraServiceServer
	GossipHandler           *gossip.GossipHandler
	NodeRingView            *ring.ConsistentHashingRing
	DataDistributionHandler *dataBalancing.DistributionHandler
}

var portInternal = os.Getenv("INTERNAL_PORT")
var peerAddresses = getPeerAddresses()
var absoluteSavePath string

// handle incoming read request
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("Received read request from %s , PageId: %s ,Date: %s, columns: %s", req.Name, req.PageId, req.Date, req.Columns)

	if s.NodeRingView == nil {
		return &pb.ReadResponse{}, fmt.Errorf("ring view is nil")
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(s.NodeRingView, currentNode, absoluteSavePath)
	ctx_read, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	//call read_path
	resp, err := c.Read(ctx_read, req)
	if err != nil {
		return &pb.ReadResponse{}, err
	}

	return resp, nil
}

// handle incoming write request
func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	//call write_path
	log.Printf("Received Write request from %s : Date %s PageId %s Event %s ComponentId %s ", req.Name, req.Date, req.PageId, req.Event, req.ComponentId)

	if s.NodeRingView == nil {
		return &pb.WriteResponse{}, fmt.Errorf("ring view is nil")
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(s.NodeRingView, currentNode, absoluteSavePath)
	ctx_write, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.Write(ctx_write, req)
	if err != nil {
		return &pb.WriteResponse{}, err
	}

	return resp, nil
}

// handle incoming gossip request
func (s *server) Gossip(ctx context.Context, req *pb.GossipMessage) (*pb.GossipAck, error) {
	return s.GossipHandler.HandleGossipMessage(ctx, req)
}

// handle incoming sync request
func (s *server) SyncData(stream pb.CassandraService_SyncDataServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error receiving stream: %v", err)
		}

		s.DataDistributionHandler.HandleDataSync(stream.Context(), req)
		resp := &pb.SyncDataResponse{
			Status:  "success",
			Message: "Processed successfully",
		}
		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("error sending stream: %v", err)
		}
	}
}

// handle delete requests
func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Printf("Received delete request: Date %s PageId %s Event %s ComponentId %s ", req.Date, req.PageId, req.Event, req.ComponentId)

	if s.NodeRingView == nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: "Failed to start ring",
		}, fmt.Errorf("ring view is nil")
	}

	var portnum, _ = strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(s.NodeRingView, currentNode, absoluteSavePath)
	ctx_delete, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := c.Delete(ctx_delete, req)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	return resp, nil
}

func main() {
	// Start the gossip protocol and gRPC server
	nodeName := os.Getenv("NODE_NAME")
	nodeID := os.Getenv("ID")
	port, _ := strconv.ParseUint(portInternal, 10, 64)

	relativePathSaveDir := fmt.Sprintf("../../internal/db/internal/data/%s.json", nodeID)
	absoluteSavePath = utils.GetPath(relativePathSaveDir)

	// Initialize the current node and gossip handler
	currentNode := &types.Node{
		ID:        nodeID,
		Name:      nodeName,
		Port:      port,
		Status:    types.NodeStatusAlive,
		IPAddress: nodeName,
	}

	ringView := ring.CreateConsistentHashingRing(currentNode, 3, 2)

	distributionHandler := dataBalancing.NewDistributionHandler(ringView, currentNode, absoluteSavePath)

	gossipFanOut := 2
	gossipTimeout := 8
	gossipInterval := 3
	gossipHandler := gossip.NewGossipHandler(currentNode, ringView, gossipFanOut, gossipTimeout, gossipInterval, distributionHandler)

	for _, address := range peerAddresses {
		parts := strings.Split(address, ":")
		pt, _ := strconv.ParseUint(parts[1], 10, 64)
		peerNode := &types.Node{
			Name:      parts[0],
			Port:      pt,
			ID:        fmt.Sprintf("node-%s", parts[0][len(parts[0])-1:]),
			IPAddress: parts[0],
			Status:    types.NodeStatusAlive,
		}
		gossipHandler.Membership.AddOrUpdateNode(peerNode, ringView)
	}

	go StartServer(ringView, gossipHandler, distributionHandler)

	// Start the gossip protocol
	go gossipHandler.Start(context.Background(), 2)

	// Start HTTP server for REST API
	go StartHTTPServer(ringView)

	// Block forever
	select {}
}

func StartServer(ringView *ring.ConsistentHashingRing, gossipHandler *gossip.GossipHandler, distributionHandler *dataBalancing.DistributionHandler) {
	log.Printf("gRPC Listening on %s", fmt.Sprintf(":%s", portInternal))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", portInternal))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterCassandraServiceServer(grpcServer, &server{
		GossipHandler:           gossipHandler,
		NodeRingView:            ringView,
		DataDistributionHandler: distributionHandler,
	})

	log.Printf("gRPC server is running on port %s", portInternal)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// New HTTP server to serve ring info
func StartHTTPServer(ringView *ring.ConsistentHashingRing) {
	http.HandleFunc("/ring", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow all origins
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		info := ringView.GetRingInfo()
		json.NewEncoder(w).Encode(info)
	})

	port := 8080
	for {
		log.Printf("HTTP server running on port %d...\n", port)
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
		if err != nil {
			log.Printf("Failed to start HTTP server on port %d: %v", port, err)
			port++
		} else {
			break
		}
	}
}

func getPeerAddresses() []string {
	peerAddressesEnv := os.Getenv("PEER_NODES")
	if peerAddressesEnv == "" {
		log.Fatal("PEER_ADDRESSES environment variable not set")
	}

	peerAddresses := strings.Split(peerAddressesEnv, ",")
	return peerAddresses
}
