package server

import (
	"context"
	"errors"
	"log"
	"net"
	"skiffdb/proto/cluster"
	"skiffdb/src/core"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func InitRaft(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	if core.DBConfig.EnableRaft {
		if err := core.StartRaft(ctx, core.DBConfig); err != nil {
			log.Fatalf("failed to start raft: %v", err)
		}
		log.Printf("Raft enabled at %s, id: %s", core.DBConfig.RaftAddr, core.DBConfig.GetRaftId())
	}

	lis, err := net.Listen("tcp", core.DBConfig.AdminAddr)
	if err != nil {
		if shutdownErr := core.ShutdownRaft(); shutdownErr != nil {
			log.Printf("failed to close raft after admin listener error: %v", shutdownErr)
		}
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	cluster.RegisterClusterAdminServer(s, &raftClusterAdminServer{})

	log.Printf("admin server listening at %v", lis.Addr())
	go func() {
		defer wg.Done()
		serveErr := s.Serve(lis)
		if err := core.ShutdownRaft(); err != nil {
			log.Printf("failed to shut down raft cleanly: %v", err)
		}
		if serveErr != nil && ctx.Err() == nil {
			log.Printf("admin server stopped unexpectedly: %v", serveErr)
		}
	}()
	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()
}

type raftClusterAdminServer struct {
	cluster.UnimplementedClusterAdminServer
}

func (s *raftClusterAdminServer) JoinCluster(ctx context.Context, req *cluster.JoinClusterRequest) (*cluster.JoinClusterResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	addr := strings.TrimSpace(req.GetAddr())

	if nodeID == "" || addr == "" {
		return nil, status.Errorf(codes.InvalidArgument, "node_id and addr are required")
	}

	log.Printf("Received JoinClusterRequest node_id=%s addr=%s", nodeID, addr)
	info, err := core.JoinCluster(nodeID, addr)
	if err != nil {
		if errors.Is(err, core.ErrRaftNotEnabled) {
			return &cluster.JoinClusterResponse{Code: cluster.JoinClusterResponseCode_RAFT_NOT_ENABLED, Cluster: toProtoCluster(info)}, nil
		}
		if nle, ok := err.(*core.NotLeaderError); ok {
			leaderInfo := &cluster.ClusterInfo{}
			leaderInfo.LeaderAdminAddr = nle.LeaderAdminAddr
			return &cluster.JoinClusterResponse{Code: cluster.JoinClusterResponseCode_NOT_LEADER, Cluster: leaderInfo}, nil
		}
		return nil, status.Errorf(codes.Internal, "join cluster: %v", err)
	}
	return &cluster.JoinClusterResponse{Cluster: toProtoCluster(info)}, nil
}

func toProtoCluster(info *core.ClusterInfo) *cluster.ClusterInfo {
	if info == nil {
		return &cluster.ClusterInfo{}
	}
	members := make([]*cluster.ClusterMember, 0, len(info.Members))
	for _, member := range info.Members {
		members = append(members, &cluster.ClusterMember{
			NodeId: member.NodeID,
			Addr:   member.Addr,
			Role:   toProtoRole(member.Role),
		})
	}
	return &cluster.ClusterInfo{
		LeaderId: info.LeaderID,
		Members:  members,
	}
}

func toProtoRole(role core.ClusterRole) cluster.ClusterRole {
	switch role {
	case core.ClusterRoleVoter:
		return cluster.ClusterRole_CLUSTER_ROLE_VOTER
	case core.ClusterRoleNonVoter:
		return cluster.ClusterRole_CLUSTER_ROLE_NON_VOTER
	default:
		return cluster.ClusterRole_CLUSTER_ROLE_UNSPECIFIED
	}
}
