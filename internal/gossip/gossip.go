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
}

// NewGossipHandler initializes a new GossipHandler for the current node.
func NewGossipHandler(currentNode *types.Node, chr *chr.ConsistentHashingRing) *GossipHandler {
	return &GossipHandler{
		NodeInfo:     currentNode,
		Membership:   NewMembership(currentNode),
		chr:          chr,
		seenMessages: make(map[string]bool),
	}
}

func (g *GossipHandler) Start(ctx context.Context, numberOfNodesToGossip int) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping gossip protocol...")
			return
		case <-ticker.C:
			g.gossip(ctx, numberOfNodesToGossip)
		}
	}
}

func (g *GossipHandler) gossip(ctx context.Context, numberOfNodesToGossip int) {
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

	targetNodeIndices := rand.Perm(len(aliveNodes))
	for i := 0; i < numberOfNodesToGossip && i < len(aliveNodes);  {
		// skip the current node
		if aliveNodes[targetNodeIndices[i]].Name == g.NodeInfo.Name {
			continue
		}
		targetNode := aliveNodes[targetNodeIndices[i]]

		log.Printf("Node[%s] Gossiping with node: %s\n", g.NodeInfo.ID, targetNode.Name)

		// Send gossip message to the target node
		message := g.createGossipMessage()

		// Send gossip message to the target node
		address := targetNode.IPAddress + ":" + strconv.FormatUint(targetNode.Port, 10)

		err := communication.SendGossipMessage(&ctx, address, message)
		if err != nil {
			log.Printf("Error gossiping with node %s; %v\n", targetNode.Name, err)
			g.Membership.MarkNodeSuspect(targetNode.Name)
		} else {
			log.Printf("Node[%s] Gossip exchange with node %s successful.\n", g.NodeInfo.ID, targetNode.Name)
			g.Membership.Heartbeat(targetNode.Name)
		}
		i++
	}
}

// createGossipMessage creates a message containing the current node's view of the cluster.
func (g *GossipHandler) createGossipMessage() *pb.GossipMessage {
	nodes := g.Membership.GetAllNodes()

	// Prepare node information for the gossip message
	nodeInfos := make(map[string]*pb.NodeInfo)

	for id, node := range nodes {
		nodeInfos[id] = &pb.NodeInfo{
			IpAddress:  node.IPAddress,
			Id:         node.ID,
			Port:       node.Port,
			Name:       node.Name,
			Status:     string(node.Status),
			LastUpdated: node.LastUpdated.Format(time.RFC3339),
		}
	}

	return &pb.GossipMessage{
		Sender:           g.NodeInfo.Name,
		MessageCreator:   g.NodeInfo.ID,
		MessageCreateTime: time.Now().Format(time.RFC3339),
		Nodes:            nodeInfos,
	}
}

// HandleGossipMessage processes an incoming gossip message.
func (g *GossipHandler) HandleGossipMessage(ctx context.Context, req *pb.GossipMessage) (*pb.GossipAck, error) {
	log.Printf("Node[%s] Received gossip message from node: %s. %s", g.NodeInfo.ID, req.Sender, req.String())

	// Deduplication: Check if message was already seen
	messageID := req.MessageCreator + req.MessageCreateTime
	g.seenMessagesMu.Lock()
	if g.seenMessages[messageID] {
		g.seenMessagesMu.Unlock()
		log.Printf("Node[%s] Ignoring duplicate gossip message from node: %s\n", g.NodeInfo.ID, req.Sender)
		return &pb.GossipAck{Ack: true}, nil
	}
	g.seenMessages[messageID] = true
	g.seenMessagesMu.Unlock()

	// Merge cluster information
	for _, nodeInfo := range req.Nodes {
		if nodeInfo.Id == g.NodeInfo.ID {
			continue
		}

		lastUpdated, err := time.Parse(time.RFC3339, nodeInfo.LastUpdated)
		if err != nil {
			log.Printf("Error parsing time: %v\n", err)
			// handle the error accordingly
		}

		updatedNode := g.Membership.AddOrUpdateNode(&types.Node{
			IPAddress: nodeInfo.IpAddress,
			ID:        nodeInfo.Id,
			Port:      nodeInfo.Port,
			Name:      nodeInfo.Name,
			Status:    types.NodeStatus(nodeInfo.Status),
			LastUpdated: lastUpdated,
		})

		// if node has not been seen before, add it to the consistent hashing ring
		if updatedNode != nil {
			log.Printf("Node[%s] Updated node: %s\n", g.NodeInfo.ID, updatedNode.String())
			nodeToUpdate := types.Node{ ID: updatedNode.ID, Name: updatedNode.Name, Port: updatedNode.Port, IPAddress: updatedNode.IPAddress, Status: updatedNode.Status, LastUpdated: time.Now()}
			if !g.chr.DoesRingContainNode(&nodeToUpdate) {
				g.chr.AddNode(nodeToUpdate)
			} else if updatedNode.Status == types.NodeStatusDead {
				g.chr.DeleteNode(nodeToUpdate)
			}
		}
	}

	go func() {
		// Propagate the gossip to two random nodes
		nodes := g.Membership.GetAllNodes()
		aliveNodes := []*types.Node{}
		for _, node := range nodes {
			if node.Status == types.NodeStatusAlive && node.Name != g.NodeInfo.Name {
				aliveNodes = append(aliveNodes, node)
			}

			if node.Status == types.NodeStatusSuspect {
				log.Printf("Node[%s] Node %s marked as suspect %v seconds ago. \n", g.NodeInfo.ID, node.Name, time.Since(node.LastUpdated).Seconds())
				// TODO @a-nnza-r change this to a configurable value
				if time.Since(node.LastUpdated) > 8 * time.Second {
					g.Membership.MarkNodeDead(node.Name)
					log.Printf("Node[%s] Marking node %s as dead\n", g.NodeInfo.ID, node.Name)
				}
			}
		}

		if len(aliveNodes) > 0 {
			targetNodeIndices := rand.Perm(len(aliveNodes))
			for i := 0; i < 2 && i < len(aliveNodes); i++ {
				targetNode := aliveNodes[targetNodeIndices[i]]
				address := targetNode.IPAddress + ":" + strconv.FormatUint(targetNode.Port, 10)
				// update the message with the current node's membership view
				req.Sender = g.NodeInfo.Name
				req.Nodes = make(map[string]*pb.NodeInfo)
				for _, node := range g.Membership.GetAllNodes() {
					req.Nodes[node.Name] = &pb.NodeInfo{
						IpAddress:  node.IPAddress,
						Id:         node.ID,
						Port:       node.Port,
						Name:       node.Name,
						Status:     string(node.Status),
						LastUpdated: node.LastUpdated.Format(time.RFC3339),
					}
				}
				err := communication.SendGossipMessage(&ctx, address, req)
				if err != nil {
					log.Printf("Error propagating gossip to node %s; %v\n", targetNode.Name, err)
				} else {
					log.Printf("Node[%s] Propagated gossip to node %s\n", g.NodeInfo.ID, targetNode.Name)
				}
			}
		}
	}()

	return &pb.GossipAck{Ack: true}, nil
}
