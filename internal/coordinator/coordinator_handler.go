package coordinator

import (
	"context"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
)

type Handler interface {
	GetRing() ring.ConsistentHashingRing
	Read(ctx context.Context, key string) (string, error)
	Write(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
}

type CoordinatorHandler struct {
	pb.UnimplementedCassandraServiceServer
	hashRing    *ring.ConsistentHashingRing
	currentNode *types.Node
	absolutePathSaveDir string
}

func NewCoordinatorHandler(ring *ring.ConsistentHashingRing, currentNode *types.Node, 	absolutePathSaveDir string) *CoordinatorHandler {
	return &CoordinatorHandler{
		hashRing:    ring,
		currentNode: currentNode,
		absolutePathSaveDir: absolutePathSaveDir,
	}
}

// GetRing returns the hash ring.
func (h CoordinatorHandler) GetRing() *ring.ConsistentHashingRing {
	return h.hashRing
}
func (h CoordinatorHandler) GetNode() *types.Node {
	return h.currentNode
}
