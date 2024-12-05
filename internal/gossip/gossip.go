package gossip

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/communication"
	"github.com/uimagine-admin/tunadb/internal/dataBalancing"
	chr "github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// GossipHandler handles the gossip protocol for a node
type GossipHandler struct {
	NodeInfo       *types.Node
	Membership     *Membership
	chr            *chr.ConsistentHashingRing
	seenMessages   map[string]bool // Tracks seen messages to avoid duplicate processing
	seenMessagesMu sync.Mutex     // Mutex for concurrent access to seenMessages

	gossipFanOut int
	deadNodeTimeout int // Timeout for marking a node as dead in seconds after being marked as suspect
	gossipInterval int // Interval for gossiping in seconds

}

// NewGossipHandler initializes a new GossipHandler for the current node.
func NewGossipHandler(currentNode *types.Node, chr *chr.ConsistentHashingRing, gossipFanOut int, deadNodeTimeout int, gossipInterval int, dh *dataBalancing.DistributionHandler) *GossipHandler {
	if gossipFanOut <= 1 {
		log.Fatalf("Invalid gossip fan out: %d, MUST CHOSE A VALUE OF AT LEAST 2. \n", gossipFanOut)
	}
	return &GossipHandler{
		NodeInfo:     currentNode,
		Membership:   NewMembership(currentNode, dh),
		chr:          chr,
		seenMessages: make(map[string]bool),
		gossipFanOut: gossipFanOut,
		deadNodeTimeout: deadNodeTimeout,
		gossipInterval: gossipInterval,
	}
}

func (g *GossipHandler) Start(ctx context.Context, gossipFanOut int) {
	ticker := time.NewTicker(time.Duration(g.gossipInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping gossip protocol...")
			return
		case <-ticker.C:
			g.gossip(ctx, gossipFanOut)
		}
	}
}

func (g *GossipHandler) gossip(ctx context.Context, gossipFanOut int) {
	nodes := g.Membership.GetAllNodes()

	// Filter alive nodes
	aliveNodes := []*types.Node{}

	for _, node := range nodes {
		if node.Status != types.NodeStatusDead && node.Name != g.NodeInfo.Name {
			aliveNodes = append(aliveNodes, node)
		}
	}

	if len(aliveNodes) == 0 {
		log.Println("No other nodes to gossip with.")
		return
	}

	targetNodeIndices := rand.Perm(len(aliveNodes))
	for i := 0; i < gossipFanOut && i < len(aliveNodes);i++  {
		// skip the current node
		if aliveNodes[targetNodeIndices[i]].Name == g.NodeInfo.Name {
			continue
		}
		targetNode := aliveNodes[targetNodeIndices[i]]

		// log.Printf("Node[%s] Gossiping with node: %s\n", g.NodeInfo.ID, targetNode.Name)

		// Send gossip message to the target node
		message := g.createGossipMessage()

		// Send gossip message to the target node
		address :=  targetNode.IPAddress + ":" + strconv.FormatUint(targetNode.Port, 10)

		err := communication.SendGossipMessage(&ctx, address, message)
		if err != nil {
			log.Printf("[%s] Error gossiping with node %s; %v\n", g.NodeInfo.ID ,targetNode.Name, err)
			g.Membership.MarkNodeSuspect(targetNode.Name)
		} else {
			// log.Printf("Node[%s] Gossip exchange with node %s successful.\n", g.NodeInfo.ID, targetNode.Name)
			g.Membership.Heartbeat(targetNode.Name)
		}

	}
}

// createGossipMessage creates a message containing the current node's view of the cluster.
func (g *GossipHandler) createGossipMessage() *pb.GossipMessage {
	nodes := g.Membership.GetAllNodes()

	// Prepare node information for the gossip message
	nodeInfos := make(map[string]*pb.NodeInfo)

	for id, node := range nodes {
		if node.Name == g.NodeInfo.Name {
			nodeInfos[id] = &pb.NodeInfo{
				IpAddress:  node.IPAddress,
				Id:         node.ID,
				Port:       node.Port,
				Name:       node.Name,
				Status:     string(node.Status),
				LastUpdated: time.Now().Format(time.RFC3339Nano),
			}
		} else {
			nodeInfos[id] = &pb.NodeInfo{
				IpAddress:  node.IPAddress,
				Id:         node.ID,
				Port:       node.Port,
				Name:       node.Name,
				Status:     string(node.Status),
				LastUpdated: node.LastUpdated.Format(time.RFC3339Nano),
			}
	}
	}

	return &pb.GossipMessage{
		Sender:           g.NodeInfo.Name,
		MessageCreator:   g.NodeInfo.ID,
		MessageCreateTime: time.Now().Format(time.RFC3339Nano),
		Nodes:            nodeInfos,
	}
}

// HandleGossipMessage processes an incoming gossip message.
func (g *GossipHandler) HandleGossipMessage(ctx context.Context, req *pb.GossipMessage) (*pb.GossipAck, error) {
	// printReceivedGossipMessage(g.NodeInfo.Name,req)

	// Deduplication: Check if message was already seen
	messageID := req.MessageCreator + req.MessageCreateTime
	g.seenMessagesMu.Lock()
	if g.seenMessages[messageID] {
		g.seenMessagesMu.Unlock()
		// log.Printf("Node[%s] Ignoring duplicate gossip message from node: %s\n", g.NodeInfo.ID, req.Sender)
		return &pb.GossipAck{Ack: true}, nil
	} else {
		g.seenMessages[messageID] = true
		g.seenMessagesMu.Unlock()
	}

	// Merge cluster information
	for _, nodeInfo := range req.Nodes {
		// Skip the current node
		if nodeInfo.Id == g.NodeInfo.ID {
			continue
		}

		lastUpdated, err := time.Parse(time.RFC3339Nano, nodeInfo.LastUpdated)
		if err != nil {
			log.Printf("Error parsing time: %v\n", err)
			// handle the error accordingly
		}

		g.Membership.AddOrUpdateNode(&types.Node{
			IPAddress: nodeInfo.IpAddress,
			ID:        nodeInfo.Id,
			Port:      nodeInfo.Port,
			Name:      nodeInfo.Name,
			Status:    types.NodeStatus(nodeInfo.Status),
			LastUpdated: lastUpdated,
		},g.chr)
	}

	go func() {
		// Propagate the gossip to two random nodes
		nodes := g.Membership.GetAllNodes()
		aliveNodes := []*types.Node{}
		for _, node := range nodes {
			if ( node.Status == types.NodeStatusAlive || node.Status == types.NodeStatusSuspect ) && node.Name != g.NodeInfo.Name {
				aliveNodes = append(aliveNodes, node)
			}
			
			if node.Status == types.NodeStatusSuspect && node.Name != g.NodeInfo.Name {
				log.Printf("Node[%s] Node %s marked as suspect %v seconds ago.[%v] \n", g.NodeInfo.ID, node.Name, time.Since(node.LastUpdated).Seconds(), node.LastUpdated )
				// TODO @a-nnza-r change this to a configurable value
				if time.Since(node.LastUpdated) > (time.Duration(g.deadNodeTimeout) * time.Second) {
					log.Printf("Node[%s] Marking node %s as dead\n", g.NodeInfo.ID, node.Name)
					g.Membership.MarkNodeDead(node.Name, g.chr)
					g.Membership.AddOrUpdateNode(&types.Node{
						IPAddress: node.IPAddress,
						ID:        node.ID,
						Port:      node.Port,
						Name:      node.Name,
						Status:    types.NodeStatusDead,
						LastUpdated: node.LastUpdated,
					}, g.chr)

				}
			}
		}

		if len(aliveNodes) > 0 {
			targetNodeIndices := rand.Perm(len(aliveNodes))
			for i := 0; i < g.gossipFanOut && i < len(aliveNodes); i++ {
				targetNode := aliveNodes[targetNodeIndices[i]]
				targetNodeAddress := targetNode.IPAddress + ":" + strconv.FormatUint(targetNode.Port, 10)
				// update the message with the current node's membership view
				req.Sender = g.NodeInfo.Name
				req.Nodes = make(map[string]*pb.NodeInfo)
				for _, node := range g.Membership.GetAllNodes() {
					if node.Name == g.NodeInfo.Name {
						req.Nodes[node.Name] = &pb.NodeInfo{
							IpAddress:  node.IPAddress,
							Id:         node.ID,
							Port:       node.Port,
							Name:       node.Name,
							Status:     string(node.Status),
							LastUpdated: time.Now().Format(time.RFC3339Nano),
						}
					} else {
						req.Nodes[node.Name] = &pb.NodeInfo{
							IpAddress:  node.IPAddress,
							Id:         node.ID,
							Port:       node.Port,
							Name:       node.Name,
							Status:     string(node.Status),
							LastUpdated: node.LastUpdated.Format(time.RFC3339Nano),
						}
					}
				}
				// printPropagatedGossipMessage(req)
				err := communication.SendGossipMessage(&ctx, targetNodeAddress, req)
				if err != nil {
					log.Printf("Node[%s] Error propagating gossip to node %s with %d; %v\n", g.NodeInfo.ID ,targetNode.Name, err, i)
				} else {
					// log.Printf("Node[%s] Propagated gossip to node %s\n", g.NodeInfo.ID, targetNode.Name)
				}
			}
		}
	}()

	return &pb.GossipAck{Ack: true}, nil
}

func printReceivedGossipMessage(receiver string,req *pb.GossipMessage){
	nodesStr := ""
	for id, node := range req.Nodes {
		nodesStr += "["  + id + " " + node.Name + " " + node.Status + " " + node.LastUpdated + "] \n"
	}
	log.Printf("[%s] RECEIVED \nSender: %s, \nMessageCreator: %s, \nMessageCreateTime: %s, \nNodes: \n%s \n \n \n", receiver , req.Sender, req.MessageCreator, req.MessageCreateTime, nodesStr)
}

func printPropagatedGossipMessage(req *pb.GossipMessage){
	nodesStr := ""
	for id, node := range req.Nodes {
		nodesStr += "["  + id + " " + node.Name + " " + node.Status + " " + node.LastUpdated + "] \n"
	}
	log.Printf("[%s] FORWARDING \nSender: %s, \nMessageCreator: %s, \nMessageCreateTime: %s, \nNodes: \n%s \n \n \n",req.Sender, req.Sender, req.MessageCreator, req.MessageCreateTime, nodesStr)
}