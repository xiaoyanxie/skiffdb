package core

import (
	"context"
	"testing"
	"time"

	raftpkg "github.com/hashicorp/raft"
)

func TestJoinClusterAddsReachableNodeAsVoter(t *testing.T) {
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

	info, err := JoinCluster(followerConfig.GetRaftId(), followerConfig.GetRaftAdvertiseAddr())
	if err != nil {
		t.Fatalf("JoinCluster() error = %v", err)
	}
	var found bool
	for _, member := range info.Members {
		if member.NodeID == followerConfig.GetRaftId() {
			found = true
			if member.Role != ClusterRoleVoter {
				t.Fatalf("joined member role = %q, want voter", member.Role)
			}
		}
	}
	if !found {
		t.Fatalf("joined member missing from cluster info: %#v", info.Members)
	}

	future := follower.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("follower GetConfiguration() error = %v", err)
	}
	var followerSuffrage raftpkg.ServerSuffrage
	for _, member := range future.Configuration().Servers {
		if member.ID == raftpkg.ServerID(followerConfig.GetRaftId()) {
			followerSuffrage = member.Suffrage
		}
	}
	if followerSuffrage != raftpkg.Voter {
		t.Fatalf("replicated follower suffrage = %v, want voter", followerSuffrage)
	}
}
