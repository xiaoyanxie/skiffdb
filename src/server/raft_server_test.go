package server

import (
	"testing"

	"skiffdb/proto/cluster"
	"skiffdb/src/core"
)

func TestToProtoClusterPreservesJoinLifecycleAndLeaderAddresses(t *testing.T) {
	info := &core.ClusterInfo{
		LeaderID:        "node-a",
		LeaderAddr:      "127.0.0.1:7001",
		LeaderAdminAddr: "127.0.0.1:50051",
		Members: []core.ClusterMember{
			{
				NodeID:       "node-b",
				Addr:         "127.0.0.1:7002",
				Role:         core.ClusterRoleNonVoter,
				State:        core.ClusterMemberStateCatchingUp,
				AppliedIndex: 41,
				TargetIndex:  42,
			},
		},
	}

	got := toProtoCluster(info)
	if got.GetLeaderId() != info.LeaderID || got.GetLeaderAddr() != info.LeaderAddr || got.GetLeaderAdminAddr() != info.LeaderAdminAddr {
		t.Fatalf("leader fields = %#v, want %#v", got, info)
	}
	if len(got.Members) != 1 {
		t.Fatalf("members = %#v, want one member", got.Members)
	}
	member := got.Members[0]
	if member.GetRole() != cluster.ClusterRole_CLUSTER_ROLE_NON_VOTER || member.GetState() != cluster.ClusterMemberState_CLUSTER_MEMBER_STATE_CATCHING_UP {
		t.Fatalf("member role/state = %v/%v, want non-voter/catching-up", member.GetRole(), member.GetState())
	}
	if member.GetAppliedIndex() != 41 || member.GetTargetIndex() != 42 {
		t.Fatalf("member progress = %d/%d, want 41/42", member.GetAppliedIndex(), member.GetTargetIndex())
	}
}
