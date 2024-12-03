package coordinator

import (
	"context"

	pb "github.com/uimagine-admin/tunadb/api"
	"github.com/uimagine-admin/tunadb/internal/db"
)

// take in a delete request only from clients - this is used for testing purposes
func (h *CoordinatorHandler) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	err := db.HandleDelete(h.GetNode().ID, req)

	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.DeleteResponse{
		Success: true,
		Message: "Row deleted successfully",
	}, nil
}
