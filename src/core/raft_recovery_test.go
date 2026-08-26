package core

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"skiffdb/src/resp"

	raftpkg "github.com/hashicorp/raft"
)

func TestRaftStartupRequiresExplicitOneTimeOperation(t *testing.T) {
	ensureRaftStopped(t)
	config := newTestRaftConfig(t, t.TempDir())
	config.RaftAddr = unusedTCPAddress(t)
	config.Bootstrap = false

	err := StartRaft(context.Background(), config)
	if err == nil || !strings.Contains(err.Error(), "requires exactly one of --bootstrap or --join") {
		t.Fatalf("StartRaft() error = %v, want explicit startup-operation error", err)
	}
	if RaftEnabled() {
		t.Fatal("failed startup left Raft enabled")
	}

	config.Bootstrap = true
	config.JoinAddr = "127.0.0.1:1"
	err = StartRaft(context.Background(), config)
	if err == nil || !strings.Contains(err.Error(), "requires exactly one of --bootstrap or --join") {
		t.Fatalf("StartRaft() with bootstrap and join error = %v, want conflicting startup-operation error", err)
	}
	config.JoinAddr = ""
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := StartRaft(ctx, config); err != nil {
		t.Fatalf("StartRaft() with explicit bootstrap error = %v", err)
	}
}

func TestRaftRestartReusesPersistedIdentityAndRejectsStaleFlags(t *testing.T) {
	ensureRaftStopped(t)
	dataDir := t.TempDir()
	address := unusedTCPAddress(t)
	config := newTestRaftConfig(t, dataDir)
	config.RaftAddr = address
	ctx, cancel := context.WithCancel(context.Background())
	if err := StartRaft(ctx, config); err != nil {
		cancel()
		t.Fatalf("initial StartRaft() error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)
	cancel()
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("initial ShutdownRaft() error = %v", err)
	}

	identityData, err := os.ReadFile(raftIdentityPath(config))
	if err != nil {
		t.Fatalf("ReadFile(identity) error = %v", err)
	}
	var identity raftIdentity
	if err := json.Unmarshal(identityData, &identity); err != nil {
		t.Fatalf("json.Unmarshal(identity) error = %v", err)
	}
	if identity.NodeID != config.GetRaftId() || identity.AdvertiseAddr != address {
		t.Fatalf("persisted identity = %#v, want node %q at %q", identity, config.GetRaftId(), address)
	}
	assertPermissions(t, raftIdentityPath(config), raftDatabaseMode)

	for _, test := range []struct {
		name      string
		bootstrap bool
		joinAddr  string
	}{
		{name: "bootstrap", bootstrap: true},
		{name: "join", joinAddr: "127.0.0.1:1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stale := &Config{
				EnableRaft: true,
				Bootstrap:  test.bootstrap,
				RaftAddr:   address,
				JoinAddr:   test.joinAddr,
				dataDir:    dataDir,
			}
			err := StartRaft(context.Background(), stale)
			if err == nil || !strings.Contains(err.Error(), "existing Raft state must be restarted without --bootstrap or --join") {
				t.Fatalf("StartRaft() error = %v, want stale-flag conflict", err)
			}
			if RaftEnabled() {
				t.Fatal("stale-flag failure left Raft enabled")
			}
		})
	}

	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("SplitHostPort(%q) error = %v", address, err)
	}
	restart := &Config{
		EnableRaft:        true,
		RaftAddr:          net.JoinHostPort("0.0.0.0", port),
		RaftAdvertiseAddr: address,
		dataDir:           dataDir,
	}
	restartCtx, restartCancel := context.WithCancel(context.Background())
	defer restartCancel()
	if err := StartRaft(restartCtx, restart); err != nil {
		t.Fatalf("restart without --raft-id error = %v", err)
	}
	if restart.GetRaftId() != config.GetRaftId() {
		t.Fatalf("reused node ID = %q, want %q", restart.GetRaftId(), config.GetRaftId())
	}
}

func TestRaftRestartRejectsIdentityAndAddressConflicts(t *testing.T) {
	ensureRaftStopped(t)
	dataDir := t.TempDir()
	address := unusedTCPAddress(t)
	config := newTestRaftConfig(t, dataDir)
	config.RaftAddr = address
	ctx, cancel := context.WithCancel(context.Background())
	if err := StartRaft(ctx, config); err != nil {
		cancel()
		t.Fatalf("initial StartRaft() error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)
	cancel()
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("initial ShutdownRaft() error = %v", err)
	}

	wrongIdentity := &Config{
		EnableRaft: true,
		raftID:     "different-node",
		RaftAddr:   address,
		dataDir:    dataDir,
	}
	if err := StartRaft(context.Background(), wrongIdentity); err == nil || !strings.Contains(err.Error(), "conflicts with persisted node identity") {
		t.Fatalf("identity-conflict StartRaft() error = %v", err)
	}

	wrongAddress := &Config{
		EnableRaft: true,
		raftID:     config.GetRaftId(),
		RaftAddr:   unusedTCPAddress(t),
		dataDir:    dataDir,
	}
	if err := StartRaft(context.Background(), wrongAddress); err == nil || !strings.Contains(err.Error(), "conflicts with persisted address") {
		t.Fatalf("address-conflict StartRaft() error = %v", err)
	}

	if err := os.Remove(raftIdentityPath(config)); err != nil {
		t.Fatalf("Remove(identity) error = %v", err)
	}
	legacyConflict := &Config{
		EnableRaft: true,
		Bootstrap:  true,
		raftID:     "different-node",
		RaftAddr:   unusedTCPAddress(t),
		dataDir:    dataDir,
	}
	if err := StartRaft(context.Background(), legacyConflict); err == nil || !strings.Contains(err.Error(), "conflicts with existing local Raft store") {
		t.Fatalf("legacy identity-conflict StartRaft() error = %v", err)
	}
}

func TestJoinClusterRejectsIdentityAndAddressConflicts(t *testing.T) {
	ensureRaftStopped(t)
	config := newTestRaftConfig(t, t.TempDir())
	config.RaftAddr = unusedTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := StartRaft(ctx, config); err != nil {
		t.Fatalf("StartRaft() error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)

	if _, err := JoinCluster(config.GetRaftId(), config.GetRaftAdvertiseAddr()); err != nil {
		t.Fatalf("idempotent JoinCluster() error = %v", err)
	}
	if _, err := JoinCluster(config.GetRaftId(), unusedTCPAddress(t)); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("same-ID JoinCluster() error = %v, want identity conflict", err)
	}
	if _, err := JoinCluster("different-node", config.GetRaftAdvertiseAddr()); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("same-address JoinCluster() error = %v, want address conflict", err)
	}
}

func ensureRaftStopped(t *testing.T) {
	t.Helper()
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("initial ShutdownRaft() error = %v", err)
	}
	t.Cleanup(func() {
		if err := ShutdownRaft(); err != nil {
			t.Errorf("cleanup ShutdownRaft() error = %v", err)
		}
	})
}

type recoveryTestNode struct {
	config *Config
	db     MemDB
	node   *RaftNode
}

func TestThreeVoterSequentialAndFullClusterRestartWithSnapshotCatchup(t *testing.T) {
	nodes := make([]*recoveryTestNode, 3)
	for index := range nodes {
		config := &Config{
			EnableRaft:        true,
			raftID:            "recovery-node-" + string(rune('a'+index)),
			RaftAddr:          unusedTCPAddress(t),
			RaftAdvertiseAddr: "",
			dataDir:           t.TempDir(),
		}
		config.RaftAdvertiseAddr = config.RaftAddr
		nodes[index] = &recoveryTestNode{config: config}
		nodes[index].db.Init()
		nodes[index].open(t)
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			if node.node != nil {
				_ = node.node.Close()
			}
		}
	})

	if err := createNewCluster(nodes[0].node, nodes[0].config); err != nil {
		t.Fatalf("createNewCluster() error = %v", err)
	}
	waitForTestNodeLeader(t, nodes, 8*time.Second)
	for index := 1; index < len(nodes); index++ {
		future := nodes[0].node.raft.AddVoter(
			raftpkg.ServerID(nodes[index].config.GetRaftId()),
			nodes[index].node.transport.LocalAddr(),
			0,
			8*time.Second,
		)
		if err := future.Error(); err != nil {
			t.Fatalf("AddVoter(node %d) error = %v", index, err)
		}
	}
	for _, node := range nodes {
		if err := persistRaftIdentity(node.config, string(node.node.transport.LocalAddr())); err != nil {
			t.Fatalf("persistRaftIdentity(%s) error = %v", node.config.GetRaftId(), err)
		}
	}
	waitForConfigurationSize(t, nodes, 3, 8*time.Second)

	leader := waitForTestNodeLeader(t, nodes, 8*time.Second)
	applyRecoveryCommand(t, leader, "before-restarts", "acknowledged")
	waitForRecoveryValue(t, nodes, "before-restarts", "acknowledged", 8*time.Second)

	// Restart each member in turn. A follower is deliberately kept offline while
	// the leader snapshots and discards the retained prefix, forcing snapshot
	// installation rather than ordinary log catch-up when that follower returns.
	followerIndex := firstFollowerIndex(nodes, leader)
	nodes[followerIndex].close(t)
	applyRecoveryCommand(t, leader, "snapshot-catchup", "installed")
	if err := leader.node.raft.Snapshot().Error(); err != nil {
		t.Fatalf("leader Snapshot() error = %v", err)
	}
	snapshots, err := leader.node.stores.snapshotStore.List()
	if err != nil || len(snapshots) == 0 {
		t.Fatalf("leader snapshots = %#v, error = %v", snapshots, err)
	}
	first, err := leader.node.stores.logStore.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex() error = %v", err)
	}
	if first <= snapshots[0].Index {
		if err := leader.node.stores.logStore.DeleteRange(first, snapshots[0].Index); err != nil {
			t.Fatalf("DeleteRange(%d, %d) error = %v", first, snapshots[0].Index, err)
		}
	}
	nodes[followerIndex].restart(t)
	waitForRecoveryValue(t, []*recoveryTestNode{nodes[followerIndex]}, "snapshot-catchup", "installed", 12*time.Second)
	installedSnapshots, err := nodes[followerIndex].node.stores.snapshotStore.List()
	if err != nil {
		t.Fatalf("follower snapshot List() error = %v", err)
	}
	if len(installedSnapshots) == 0 || installedSnapshots[0].Index < snapshots[0].Index {
		t.Fatalf("follower snapshots = %#v, want an installed snapshot at or after index %d", installedSnapshots, snapshots[0].Index)
	}

	for index := range nodes {
		if index == followerIndex {
			continue
		}
		nodes[index].restart(t)
		leader = waitForTestNodeLeader(t, nodes, 10*time.Second)
		applyRecoveryCommand(t, leader, "sequential-restart", "complete")
	}
	waitForRecoveryValue(t, nodes, "sequential-restart", "complete", 10*time.Second)
	waitForConfigurationSize(t, nodes, 3, 8*time.Second)

	for _, node := range nodes {
		node.close(t)
		node.db.Init()
	}
	for _, node := range nodes {
		node.open(t)
	}
	leader = waitForTestNodeLeader(t, nodes, 12*time.Second)
	waitForConfigurationSize(t, nodes, 3, 8*time.Second)
	waitForRecoveryValue(t, nodes, "before-restarts", "acknowledged", 12*time.Second)
	waitForRecoveryValue(t, nodes, "snapshot-catchup", "installed", 12*time.Second)
	waitForRecoveryValue(t, nodes, "sequential-restart", "complete", 12*time.Second)
	applyRecoveryCommand(t, leader, "after-full-restart", "writable")
	waitForRecoveryValue(t, nodes, "after-full-restart", "writable", 10*time.Second)
}

func (testNode *recoveryTestNode) open(t *testing.T) {
	t.Helper()
	stores, err := newPersistentRaftStores(testNode.config)
	if err != nil {
		t.Fatalf("newPersistentRaftStores(%s) error = %v", testNode.config.GetRaftId(), err)
	}
	node, err := initRaftNodeWithDB(testNode.config, stores, &testNode.db)
	if err != nil {
		_ = stores.Close()
		t.Fatalf("initRaftNodeWithDB(%s) error = %v", testNode.config.GetRaftId(), err)
	}
	testNode.node = node
}

func (testNode *recoveryTestNode) close(t *testing.T) {
	t.Helper()
	if testNode.node == nil {
		return
	}
	if err := testNode.node.Close(); err != nil {
		t.Fatalf("Close(%s) error = %v", testNode.config.GetRaftId(), err)
	}
	testNode.node = nil
}

func (testNode *recoveryTestNode) restart(t *testing.T) {
	t.Helper()
	testNode.close(t)
	testNode.db.Init()
	testNode.open(t)
	initialized, err := validatePersistedRaftConfiguration(testNode.node, testNode.config)
	if err != nil {
		t.Fatalf("validatePersistedRaftConfiguration(%s) error = %v", testNode.config.GetRaftId(), err)
	}
	if !initialized {
		t.Fatalf("restarted node %s was not initialized", testNode.config.GetRaftId())
	}
}

func waitForTestNodeLeader(t *testing.T, nodes []*recoveryTestNode, timeout time.Duration) *recoveryTestNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.node != nil && node.node.raft.State() == raftpkg.Leader {
				return node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("three-voter cluster did not elect a leader before timeout")
	return nil
}

func waitForConfigurationSize(t *testing.T, nodes []*recoveryTestNode, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allMatch := true
		for _, node := range nodes {
			future := node.node.raft.GetConfiguration()
			if future.Error() != nil || len(future.Configuration().Servers) != want {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cluster configuration did not reach %d members", want)
}

func firstFollowerIndex(nodes []*recoveryTestNode, leader *recoveryTestNode) int {
	for index, node := range nodes {
		if node != leader {
			return index
		}
	}
	return -1
}

func applyRecoveryCommand(t *testing.T, leader *recoveryTestNode, key, value string) {
	t.Helper()
	if got := applyCmdViaRaftNode(leader.node, &Cmd{Op: "SET", Args: []string{key, value}}, 8*time.Second); got != resp.Ok {
		t.Fatalf("apply %q=%q response = %q, want %q", key, value, got, resp.Ok)
	}
}

func waitForRecoveryValue(t *testing.T, nodes []*recoveryTestNode, key, value string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allMatch := true
		for _, node := range nodes {
			got := node.db.Get(key)
			if got == nil || *got != value {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("not all nodes restored %q=%q before timeout", key, value)
}
