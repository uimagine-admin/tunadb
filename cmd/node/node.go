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

// handle incoming read request (gRPC)
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	log.Printf("Received gRPC read request from %s , PageId: %s ,Date: %s, columns: %s", req.Name, req.PageId, req.Date, req.Columns)

	if s.NodeRingView == nil {
		return &pb.ReadResponse{}, fmt.Errorf("ring view is nil")
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(s.NodeRingView, currentNode, absoluteSavePath)
	ctx_read, _ := context.WithTimeout(context.Background(), time.Second)
	// ctx_read, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()

	resp, err := c.Read(ctx_read, req)
	if err != nil {
		return &pb.ReadResponse{}, err
	}

	return resp, nil
}

// handle incoming write request (gRPC)
func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	log.Printf("Received gRPC Write request from %s : Date %s PageId %s Event %s ComponentId %s ",
		req.Name, req.Date, req.PageId, req.Event, req.ComponentId)

	if s.NodeRingView == nil {
		return &pb.WriteResponse{}, fmt.Errorf("ring view is nil")
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(s.NodeRingView, currentNode, absoluteSavePath)
	ctx_write, _ := context.WithTimeout(context.Background(), time.Second)
	// ctx_write, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()

	resp, err := c.Write(ctx_write, req)
	if err != nil {
		return &pb.WriteResponse{}, err
	}

	return resp, nil
}

// for read repair
func (s *server) BulkWrite(ctx context.Context, req *pb.BulkWriteRequest) (*pb.BulkWriteResponse, error) {
	//call write_path
	log.Printf("Received Bulk Write request from %s : Data %v ", req.Name, req.Data)

	if s.NodeRingView == nil {
		return &pb.BulkWriteResponse{}, fmt.Errorf("ring view is nil")
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(s.NodeRingView, currentNode, absoluteSavePath)
	//call write_path
	ctx_write, _ := context.WithTimeout(context.Background(), time.Second)
	resp, err := c.BulkWrite(ctx_write, req)
	if err != nil {
		return &pb.BulkWriteResponse{}, err
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
		// Receive messages from the stream
		req, err := stream.Recv()
		if err == io.EOF {
			// End of stream
			return nil
		}
		if err != nil {
			return fmt.Errorf("error receiving stream: %v", err)
		}

		s.DataDistributionHandler.HandleDataSync(stream.Context(), req)

		// Send a response back
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
	log.Printf("Received gRPC delete request: Date %s PageId %s Event %s ComponentId %s ",
		req.Date, req.PageId, req.Event, req.ComponentId)

	if s.NodeRingView == nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: "Failed to start ring",
		}, fmt.Errorf("ring view is nil")
	}

	var portnum, _ = strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}

	c := coordinator.NewCoordinatorHandler(s.NodeRingView, currentNode, absoluteSavePath)
	ctx_delete, _ := context.WithTimeout(context.Background(), time.Second)
	// ctx_delete, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()

	resp, err := c.Delete(ctx_delete, req)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	return resp, nil
}

// Other gRPC handlers omitted for brevity...

func main() {
	nodeName := os.Getenv("NODE_NAME")
	nodeID := os.Getenv("ID")
	port, _ := strconv.ParseUint(portInternal, 10, 64)

	relativePathSaveDir := fmt.Sprintf("../../internal/db/internal/data/%s.json", nodeID)
	absoluteSavePath = utils.GetPath(relativePathSaveDir)

	currentNode := &types.Node{
		ID:        nodeID,
		Name:      nodeName,
		Port:      port,
		Status:    types.NodeStatusAlive,
		IPAddress: nodeName,
	}

	ringView := ring.CreateConsistentHashingRing(currentNode, 3, 2)
	distributionHandler := dataBalancing.NewDistributionHandler(ringView, currentNode, absoluteSavePath)

	// Gossip parameters
	gossipFanOut := 2
	gossipTimeout := 8
	gossipInterval := 3
	gossipHandler := gossip.NewGossipHandler(currentNode, ringView, gossipFanOut, gossipTimeout, gossipInterval, distributionHandler)

	// Add peers
	for _, address := range peerAddresses {
		parts := strings.Split(address, ":")
		port, _ := strconv.ParseUint(parts[1], 10, 64)
		peerNode := &types.Node{
			Name:      parts[0],
			Port:      port,
			ID:        fmt.Sprintf("node-%s", parts[0][len(parts[0])-1:]),
			IPAddress: parts[0],
			Status:    types.NodeStatusAlive,
		}
		gossipHandler.Membership.AddOrUpdateNode(peerNode, ringView)
	}

	// Start gRPC server
	go StartServer(ringView, gossipHandler, distributionHandler)

	// Start the gossip protocol
	go gossipHandler.Start(context.Background(), 2)

	// Start HTTP server
	go StartHTTPServer2(ringView, distributionHandler)

	select {}
}

func StartServer(ringView *ring.ConsistentHashingRing, gossipHandler *gossip.GossipHandler, distributionHandler *dataBalancing.DistributionHandler) {
	log.Printf("Listening GRPC on %s", fmt.Sprintf(":%s", portInternal))
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
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

// --------------------- HTTP SERVER CODE --------------------- //

func StartHTTPServer2(ringView *ring.ConsistentHashingRing, distributionHandler *dataBalancing.DistributionHandler) {
	httpPort := "8090" // or from environment variable
	log.Printf("HTTP server running on port %s", httpPort)

	// Middleware to handle CORS
	corsMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

			// Handle preflight requests
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	// Handlers
	http.Handle("/read", corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleReadRequest(w, r, ringView, distributionHandler)
	})))
	http.Handle("/write", corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleWriteRequest(w, r, ringView, distributionHandler)
	})))
	http.Handle("/delete", corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleDeleteRequest(w, r, ringView, distributionHandler)
	})))

	// Start the server
	if err := http.ListenAndServe(":"+httpPort, nil); err != nil {
		log.Fatalf("Failed to run HTTP server: %v", err)
	}
}

func handleReadRequest(w http.ResponseWriter, r *http.Request, ringView *ring.ConsistentHashingRing, distributionHandler *dataBalancing.DistributionHandler) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var req pb.ReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}
	c := coordinator.NewCoordinatorHandler(ringView, currentNode, absoluteSavePath)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.Read(ctx, &req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading: %v", err), http.StatusInternalServerError)
		return
	}

	writeJSONResponse(w, resp)
}

func handleWriteRequest(w http.ResponseWriter, r *http.Request, ringView *ring.ConsistentHashingRing, distributionHandler *dataBalancing.DistributionHandler) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var req pb.WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}
	c := coordinator.NewCoordinatorHandler(ringView, currentNode, absoluteSavePath)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.Write(ctx, &req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error writing: %v", err), http.StatusInternalServerError)
		return
	}

	writeJSONResponse(w, resp)
}

func handleDeleteRequest(w http.ResponseWriter, r *http.Request, ringView *ring.ConsistentHashingRing, distributionHandler *dataBalancing.DistributionHandler) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var req pb.DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	portnum, _ := strconv.ParseUint(portInternal, 10, 64)
	currentNode := &types.Node{ID: os.Getenv("ID"), Name: os.Getenv("NODE_NAME"), IPAddress: "", Port: portnum}
	c := coordinator.NewCoordinatorHandler(ringView, currentNode, absoluteSavePath)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.Delete(ctx, &req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error deleting: %v", err), http.StatusInternalServerError)
		return
	}

	writeJSONResponse(w, resp)
}

func writeJSONResponse(w http.ResponseWriter, resp interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "Failed to write response", http.StatusInternalServerError)
	}
}

// ------------------------------------------------------------ //

func getPeerAddresses() []string {
	peerAddressesEnv := os.Getenv("PEER_NODES")
	if peerAddressesEnv == "" {
		log.Fatal("PEER_ADDRESSES environment variable not set")
	}

	peerAddresses := strings.Split(peerAddressesEnv, ",")
	return peerAddresses
}
