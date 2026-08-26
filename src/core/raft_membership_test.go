package core

import (
	"context"
	"testing"
	"time"

	raftpkg "github.com/hashicorp/raft"
)

func TestJoinClusterPromotesCaughtUpNonVoter(t *testing.T) {
	ensureRaftStopped(t)
	ResetMemDB()
	leaderConfig := newTestRaftConfig(t, t.TempDir())
	leaderConfig.RaftAddr = unusedTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := StartRaft(ctx, leaderConfig); err != nil {
		t.Fatalf("StartRaft(leader) error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)

	followerConfig := &Config{
		EnableRaft:        true,
		raftID:            "joining-voter",
		RaftAddr:          unusedTCPAddress(t),
		RaftAdvertiseAddr: "",
		dataDir:           t.TempDir(),
	}
	followerConfig.RaftAdvertiseAddr = followerConfig.RaftAddr
	stores, err := newPersistentRaftStores(followerConfig)
	if err != nil {
		t.Fatalf("newPersistentRaftStores(follower) error = %v", err)
	}
	var followerDB MemDB
	followerDB.Init()
	follower, err := initRaftNodeWithDB(followerConfig, stores, &followerDB)
	if err != nil {
		_ = stores.Close()
		t.Fatalf("initRaftNodeWithDB(follower) error = %v", err)
	}
	defer follower.Close()

	info, err := JoinClusterWithProgress(followerConfig.GetRaftId(), followerConfig.GetRaftAdvertiseAddr(), 0)
	if err != nil {
		t.Fatalf("initial JoinClusterWithProgress() error = %v", err)
	}
	joining := requireClusterMember(t, info, followerConfig.GetRaftId())
	if joining.Role != ClusterRoleNonVoter || joining.State != ClusterMemberStateJoining {
		t.Fatalf("initial joined member = %#v, want joining non-voter", joining)
	}
	if joining.TargetIndex == 0 {
		t.Fatal("initial joined member has no catch-up target")
	}
	assertServerSuffrage(t, raftNode, followerConfig.GetRaftId(), raftpkg.Nonvoter)

	deadline := time.Now().Add(8 * time.Second)
	for follower.raft.AppliedIndex() < joining.TargetIndex && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if follower.raft.AppliedIndex() < joining.TargetIndex {
		t.Fatalf("follower applied index = %d, want at least target %d", follower.raft.AppliedIndex(), joining.TargetIndex)
	}

	info, err = JoinClusterWithProgress(followerConfig.GetRaftId(), followerConfig.GetRaftAdvertiseAddr(), follower.raft.AppliedIndex())
	if err != nil {
		t.Fatalf("promotion JoinClusterWithProgress() error = %v", err)
	}
	promoted := requireClusterMember(t, info, followerConfig.GetRaftId())
	if promoted.Role != ClusterRoleVoter || promoted.State != ClusterMemberStateVoter {
		t.Fatalf("promoted member = %#v, want voter", promoted)
	}

	duplicateInfo, err := JoinClusterWithProgress(followerConfig.GetRaftId(), followerConfig.GetRaftAdvertiseAddr(), follower.raft.AppliedIndex())
	if err != nil {
		t.Fatalf("duplicate JoinClusterWithProgress() error = %v", err)
	}
	memberCount := 0
	for _, member := range duplicateInfo.Members {
		if member.NodeID == followerConfig.GetRaftId() {
			memberCount++
		}
	}
	if memberCount != 1 {
		t.Fatalf("duplicate join produced %d entries for %q", memberCount, followerConfig.GetRaftId())
	}
	assertServerSuffrage(t, raftNode, followerConfig.GetRaftId(), raftpkg.Voter)
	waitForServerSuffrage(t, follower, followerConfig.GetRaftId(), raftpkg.Voter, 8*time.Second)
}

func TestJoinClusterDoesNotPromoteStalledUnreachableNode(t *testing.T) {
	ensureRaftStopped(t)
	ResetMemDB()
	leaderConfig := newTestRaftConfig(t, t.TempDir())
	leaderConfig.RaftAddr = unusedTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := StartRaft(ctx, leaderConfig); err != nil {
		t.Fatalf("StartRaft(leader) error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)

	nodeID := "unreachable-joiner"
	address := unusedTCPAddress(t)
	info, err := JoinClusterWithProgress(nodeID, address, 0)
	if err != nil {
		t.Fatalf("initial JoinClusterWithProgress() error = %v", err)
	}
	joining := requireClusterMember(t, info, nodeID)
	if joining.Role != ClusterRoleNonVoter || joining.State != ClusterMemberStateJoining {
		t.Fatalf("initial unreachable member = %#v, want joining non-voter", joining)
	}
	if joining.TargetIndex < 2 {
		t.Fatalf("catch-up target = %d, want an index with a lagging predecessor", joining.TargetIndex)
	}

	laggingIndex := joining.TargetIndex - 1
	info, err = JoinClusterWithProgress(nodeID, address, laggingIndex)
	if err != nil {
		t.Fatalf("lagging JoinClusterWithProgress() error = %v", err)
	}
	catchingUp := requireClusterMember(t, info, nodeID)
	if catchingUp.State != ClusterMemberStateCatchingUp || catchingUp.Role != ClusterRoleNonVoter {
		t.Fatalf("lagging member = %#v, want catching-up non-voter", catchingUp)
	}

	raftNode.joinProgressMu.Lock()
	raftNode.joinProgress[raftpkg.ServerID(nodeID)].lastProgressAt = time.Now().Add(-raftJoinFailureTimeout)
	raftNode.joinProgressMu.Unlock()
	info, err = buildClusterInfo()
	if err != nil {
		t.Fatalf("buildClusterInfo() error = %v", err)
	}
	failed := requireClusterMember(t, info, nodeID)
	if failed.State != ClusterMemberStateFailed || failed.Role != ClusterRoleNonVoter {
		t.Fatalf("stalled member = %#v, want failed non-voter", failed)
	}

	info, err = JoinClusterWithProgress(nodeID, address, laggingIndex)
	if err != nil {
		t.Fatalf("duplicate stalled JoinClusterWithProgress() error = %v", err)
	}
	stillFailed := requireClusterMember(t, info, nodeID)
	if stillFailed.State != ClusterMemberStateFailed || stillFailed.Role != ClusterRoleNonVoter {
		t.Fatalf("duplicate stalled member = %#v, want failed non-voter", stillFailed)
	}
	assertServerSuffrage(t, raftNode, nodeID, raftpkg.Nonvoter)
}

func requireClusterMember(t *testing.T, info *ClusterInfo, nodeID string) ClusterMember {
	t.Helper()
	if info == nil {
		t.Fatal("cluster info is nil")
	}
	for _, member := range info.Members {
		if member.NodeID == nodeID {
			return member
		}
	}
	t.Fatalf("member %q missing from cluster info: %#v", nodeID, info.Members)
	return ClusterMember{}
}

func assertServerSuffrage(t *testing.T, node *RaftNode, nodeID string, want raftpkg.ServerSuffrage) {
	t.Helper()
	future := node.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("GetConfiguration() error = %v", err)
	}
	for _, member := range future.Configuration().Servers {
		if member.ID == raftpkg.ServerID(nodeID) {
			if member.Suffrage != want {
				t.Fatalf("member %q suffrage = %v, want %v", nodeID, member.Suffrage, want)
			}
			return
		}
	}
	t.Fatalf("member %q missing from Raft configuration", nodeID)
}

func waitForServerSuffrage(t *testing.T, node *RaftNode, nodeID string, want raftpkg.ServerSuffrage, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		future := node.raft.GetConfiguration()
		if future.Error() == nil {
			for _, member := range future.Configuration().Servers {
				if member.ID == raftpkg.ServerID(nodeID) && member.Suffrage == want {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	assertServerSuffrage(t, node, nodeID, want)
}
