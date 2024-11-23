package gossip

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"time"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/communication"
	chr "github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
)

// GossipHandler handles the gossip protocol for a node
type GossipHandler struct {
	NodeInfo   *types.Node   // Information about the current node
	Membership *Membership   // Cluster membership management
	chr 	 *chr.ConsistentHashingRing
}

// NewGossipHandler initializes a new GossipHandler for the current node.
func NewGossipHandler(currentNode *types.Node, chr *chr.ConsistentHashingRing) *GossipHandler {
	return &GossipHandler{
		NodeInfo:   currentNode,
		Membership: NewMembership(currentNode),
		chr: chr,
	}
}

// Start initiates the gossip protocol.
func (g *GossipHandler) Start(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second) // Gossip every 3 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping gossip protocol...")
			return
		case <-ticker.C:
			g.gossip(ctx)
		}
	}
}

// Gossip exchanges node information with a random node in the cluster.
func (g *GossipHandler) gossip(ctx context.Context) {
	nodes := g.Membership.GetAllNodes()

	// Filter alive nodes
	aliveNodes := []*types.Node{}
	for _, node := range nodes {
		if node.Status == types.NodeStatusAlive && node.Name != g.NodeInfo.Name {
			aliveNodes = append(aliveNodes, node)
		}
	}

	if len(aliveNodes) == 0 {
		log.Println("No other nodes to gossip with.")
		return
	}

	// Choose a random node to gossip with
	targetNode := aliveNodes[rand.Intn(len(aliveNodes))]

	log.Printf("Node[%s] Gossiping with node: %s\n", g.NodeInfo.ID ,targetNode.Name)

	// Create a gossip message
	message := g.createGossipMessage()

	// Send gossip message to the target node
	address := targetNode.IPAddress + ":" + strconv.FormatUint(targetNode.Port, 10)
	err := communication.SendGossipMessage(&ctx , address, message)
	if err != nil {
		log.Printf("Error gossiping with node %s; %v\n", targetNode.Name, err)

		// Mark the node as suspect
		g.Membership.MarkNodeSuspect(targetNode.Name)
	} else {
		log.Printf("Node[%s] Gossip exchange with node %s successful.\n", g.NodeInfo.ID, targetNode.Name)

		// Update heartbeat for the target node
		g.Membership.Heartbeat(targetNode.Name)
	}
}

// createGossipMessage creates a message containing the current node's view of the cluster.
func (g *GossipHandler) createGossipMessage() *pb.GossipMessage {
	nodes := g.Membership.GetAllNodes()

	// Prepare node information for the gossip message
	nodeInfos := make(map[string]*pb.NodeInfo)
	for id, node := range nodes {
		nodeInfos[id] = &pb.NodeInfo{
			IpAddress: node.IPAddress,
			Id:    node.ID,
			Port: 	  node.Port,
			Name:   node.Name,
			Status: string(node.Status),
			LastUpdated: node.LastUpdated.Format(time.RFC3339),
		}
	}

	return &pb.GossipMessage{
		Sender: g.NodeInfo.Name,
		Nodes:  nodeInfos,
	}
}

// HandleGossipMessage processes an incoming gossip message.
func (g *GossipHandler) HandleGossipMessage(ctx context.Context, req *pb.GossipMessage) (*pb.GossipAck, error) {
	log.Printf("Node[%s] Received gossip message from node: %s\n",g.NodeInfo.ID, req.Sender)


	// Merge cluster information
	for _ , nodeInfo := range req.Nodes {
		if nodeInfo.Id == g.NodeInfo.ID {
			continue
		}
		updatedNode := g.Membership.AddOrUpdateNode(&types.Node{
			IPAddress: nodeInfo.IpAddress,
			ID:    nodeInfo.Id,
			Port:   nodeInfo.Port,
			Name:   nodeInfo.Name,
			Status: types.NodeStatus(nodeInfo.Status),
		})

		// if node has not been seen before, add it to the consistent hashing ring
		if updatedNode != nil {
			log.Printf("Node[%s] Updated node: %s\n", g.NodeInfo.ID, updatedNode.String())
			if !g.chr.DoesRingContainNode(updatedNode) {
				newNode := types.Node{ Name: updatedNode.Name, Port: updatedNode.Port }
				g.chr.AddNode(newNode)
			}else if updatedNode.Status == types.NodeStatusDead {
				nodeToDelete := types.Node{ Name: updatedNode.Name, Port: updatedNode.Port }
				g.chr.DeleteNode(nodeToDelete)
			}


		}
	}

	return &pb.GossipAck{Ack: true}, nil
}
