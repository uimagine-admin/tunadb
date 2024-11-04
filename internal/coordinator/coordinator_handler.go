package coordinator

import (
	"context"

	"github.com/uimagine-admin/tunadb/internal/ring"
	"github.com/uimagine-admin/tunadb/internal/types"
)

type Handler interface {
	GetRing() ring.ConsistentHashingRing
	Read(ctx context.Context, key string) (string, error)
	Write(ctx context.Context, key, value string) error
}

type CoordinatorHandler struct {
	hashRing    *ring.ConsistentHashingRing
	currentNode *types.Node
}

func NewCoordinatorHandler(ring *ring.ConsistentHashingRing, currentNode *types.Node) *CoordinatorHandler {
	return &CoordinatorHandler{
		hashRing:    ring,
		currentNode: currentNode,
	}
}

// GetRing returns the hash ring.
func (h CoordinatorHandler) GetRing() *ring.ConsistentHashingRing {
	return h.hashRing
}
func (h CoordinatorHandler) GetNode() *types.Node {
	return h.currentNode
}
