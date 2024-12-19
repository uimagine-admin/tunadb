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

var Reset = "\033[0m"

// Client-to-Node Messages
var ClientReadRequest = "\033[38;5;240m"  // Dark Gray
var ClientWriteRequest = "\033[38;5;241m" // Darker Blue
var ClientDeleteRequest = "\033[38;5;242m" // Dark Olive Green
var ClientBulkWriteRequest = "\033[38;5;243m" // Dark Steel Blue

// Node-to-Client Responses
var NodeReadResponse = "\033[38;5;250m"   // Bright Gray
var NodeWriteResponse = "\033[38;5;252m"  // Light Olive Green
var NodeDeleteResponse = "\033[38;5;254m" // Bright Steel Blue
var NodeBulkWriteResponse = "\033[38;5;253m" // Light Aqua

// Node-to-Node Messages (Coordinator to Node or Node to Node)
var CoordinatorReadRequest = "\033[38;5;245m"  // Medium Gray
var CoordinatorWriteRequest = "\033[38;5;244m" // Medium Blue
var CoordinatorDeleteRequest = "\033[38;5;246m" // Medium Olive Green
var CoordinatorBulkWriteRequest = "\033[38;5;247m" // Medium Steel Blue

// Other Shared Colors
var GeneralError = "\033[38;5;196m"          // Bright Red
var GeneralInfo = "\033[38;5;33m"            // Blue
var GeneralSuccess = "\033[38;5;40m"         // Green
var GeneralDebug = "\033[38;5;99m"           // Purple