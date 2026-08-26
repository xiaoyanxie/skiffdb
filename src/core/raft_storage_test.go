package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	raftpkg "github.com/hashicorp/raft"
	bbolt "go.etcd.io/bbolt"
)

var (
	currentTermKey  = []byte("CurrentTerm")
	lastVoteTermKey = []byte("LastVoteTerm")
	lastVoteCandKey = []byte("LastVoteCand")
)

func TestPersistentRaftStoreLocksNodeDirectory(t *testing.T) {
	config := newTestRaftConfig(t, t.TempDir())
	stores, err := newPersistentRaftStores(config)
	if err != nil {
		t.Fatalf("newPersistentRaftStores() error = %v", err)
	}

	nodeDir, err := raftNodeDataDir(config)
	if err != nil {
		t.Fatalf("raftNodeDataDir() error = %v", err)
	}
	assertPermissions(t, nodeDir, raftDirectoryMode)
	assertPermissions(t, filepath.Join(nodeDir, "raft.db"), raftDatabaseMode)

	started := time.Now()
	_, err = newPersistentRaftStores(config)
	if err == nil {
		t.Fatal("second newPersistentRaftStores() unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Fatalf("second open error = %q, want an already-in-use error", err)
	}
	if elapsed := time.Since(started); elapsed > 2*raftStoreOpenDelay {
		t.Fatalf("second open took %s, want a bounded lock timeout", elapsed)
	}

	if err := stores.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	reopened, err := newPersistentRaftStores(config)
	if err != nil {
		t.Fatalf("reopen after Close() error = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close() error = %v", err)
	}
}

func TestPersistentRaftStoreUsesNodeSpecificDirectories(t *testing.T) {
	dataDir := t.TempDir()
	firstConfig := newTestRaftConfig(t, dataDir)
	secondConfig := newTestRaftConfig(t, dataDir)
	secondConfig.raftID = "second-node"

	first, err := newPersistentRaftStores(firstConfig)
	if err != nil {
		t.Fatalf("open first node store error = %v", err)
	}
	defer first.Close()
	second, err := newPersistentRaftStores(secondConfig)
	if err != nil {
		t.Fatalf("open second node store under the same data directory error = %v", err)
	}
	defer second.Close()

	firstDir, _ := raftNodeDataDir(firstConfig)
	secondDir, _ := raftNodeDataDir(secondConfig)
	if firstDir == secondDir {
		t.Fatalf("node-specific directories are equal: %q", firstDir)
	}
}

func TestPersistentRaftStoreProcessLock(t *testing.T) {
	const (
		helperFlag = "SKIFFDB_RAFT_LOCK_HELPER"
		helperDir  = "SKIFFDB_RAFT_LOCK_DIR"
	)
	if os.Getenv(helperFlag) == "1" {
		config := &Config{raftID: "process-lock-node", dataDir: os.Getenv(helperDir)}
		stores, err := newPersistentRaftStores(config)
		if err == nil {
			_ = stores.Close()
			t.Fatal("child process unexpectedly opened the locked Raft store")
		}
		if !strings.Contains(err.Error(), "already in use") {
			t.Fatalf("child process error = %q, want an already-in-use error", err)
		}
		return
	}

	config := &Config{raftID: "process-lock-node", dataDir: t.TempDir()}
	stores, err := newPersistentRaftStores(config)
	if err != nil {
		t.Fatalf("newPersistentRaftStores() error = %v", err)
	}
	defer stores.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestPersistentRaftStoreProcessLock$")
	command.Env = append(os.Environ(), helperFlag+"=1", helperDir+"="+config.dataDir)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("lock-check child process failed: %v\n%s", err, output)
	}
}

func TestPersistentRaftStoreRejectsCorruption(t *testing.T) {
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("initial ShutdownRaft() error = %v", err)
	}
	config := newTestRaftConfig(t, t.TempDir())
	nodeDir, err := raftNodeDataDir(config)
	if err != nil {
		t.Fatalf("raftNodeDataDir() error = %v", err)
	}
	if err := os.MkdirAll(nodeDir, raftDirectoryMode); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	dbPath := filepath.Join(nodeDir, "raft.db")
	if err := os.WriteFile(dbPath, []byte("not a bbolt database"), raftDatabaseMode); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	err = StartRaft(context.Background(), config)
	if err == nil {
		t.Fatal("StartRaft() unexpectedly opened corrupt data")
	}
	if !strings.Contains(err.Error(), "open raft store") || !strings.Contains(err.Error(), dbPath) {
		t.Fatalf("corrupt store error = %q, want operation and database path", err)
	}
	if RaftEnabled() {
		t.Fatal("RaftEnabled() = true after corrupt-store startup failure")
	}
}

func TestStartRaftReportsUnusableDataDirectory(t *testing.T) {
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("initial ShutdownRaft() error = %v", err)
	}
	dataPath := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(dataPath, []byte("file"), raftDatabaseMode); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	config := newTestRaftConfig(t, dataPath)
	err := StartRaft(context.Background(), config)
	if err == nil {
		t.Fatal("StartRaft() unexpectedly accepted an unusable data directory")
	}
	if !strings.Contains(err.Error(), "create raft data directory") || !strings.Contains(err.Error(), dataPath) {
		t.Fatalf("unusable directory error = %q, want operation and data path", err)
	}
}

func TestStartRaftRejectsIncompatibleLogEncoding(t *testing.T) {
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("initial ShutdownRaft() error = %v", err)
	}
	config := newTestRaftConfig(t, t.TempDir())
	stores, err := newPersistentRaftStores(config)
	if err != nil {
		t.Fatalf("newPersistentRaftStores() error = %v", err)
	}
	if err := stores.logStore.StoreLog(&raftpkg.Log{Index: 1, Term: 1, Type: raftpkg.LogCommand, Data: []byte("valid")}); err != nil {
		t.Fatalf("StoreLog() error = %v", err)
	}
	if err := stores.stableStore.SetUint64(currentTermKey, 1); err != nil {
		t.Fatalf("SetUint64() error = %v", err)
	}
	if err := stores.Close(); err != nil {
		t.Fatalf("stores.Close() error = %v", err)
	}

	nodeDir, _ := raftNodeDataDir(config)
	database, err := bbolt.Open(filepath.Join(nodeDir, "raft.db"), raftDatabaseMode, nil)
	if err != nil {
		t.Fatalf("bbolt.Open() error = %v", err)
	}
	err = database.Update(func(transaction *bbolt.Tx) error {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, 1)
		return transaction.Bucket([]byte("logs")).Put(key, []byte("incompatible log encoding"))
	})
	if err != nil {
		_ = database.Close()
		t.Fatalf("corrupt log transaction error = %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("database.Close() error = %v", err)
	}

	err = StartRaft(context.Background(), config)
	if err == nil {
		t.Fatal("StartRaft() unexpectedly accepted incompatible log encoding")
	}
	if !strings.Contains(err.Error(), "failed to create raft") || !strings.Contains(err.Error(), "last log") {
		t.Fatalf("incompatible log error = %q, want a clear Raft log decode failure", err)
	}
	if RaftEnabled() {
		t.Fatal("RaftEnabled() = true after incompatible-store startup failure")
	}
}

func TestRaftNodeDataDirValidation(t *testing.T) {
	tests := []struct {
		name   string
		nodeID string
	}{
		{name: "missing", nodeID: ""},
		{name: "parent traversal", nodeID: ".."},
		{name: "slash", nodeID: "nodes/one"},
		{name: "backslash", nodeID: `nodes\one`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := raftNodeDataDir(&Config{raftID: test.nodeID, dataDir: t.TempDir()})
			if err == nil {
				t.Fatalf("raftNodeDataDir() accepted node ID %q", test.nodeID)
			}
		})
	}
}

func TestInmemRaftStoresRemainAvailableForTests(t *testing.T) {
	stores := newInmemRaftStores()
	hasState, err := raftpkg.HasExistingState(stores.logStore, stores.stableStore, stores.snapshotStore)
	if err != nil {
		t.Fatalf("HasExistingState() error = %v", err)
	}
	if hasState {
		t.Fatal("new in-memory stores unexpectedly contain state")
	}
	if err := stores.stableStore.SetUint64(currentTermKey, 3); err != nil {
		t.Fatalf("SetUint64() error = %v", err)
	}
	hasState, err = raftpkg.HasExistingState(stores.logStore, stores.stableStore, stores.snapshotStore)
	if err != nil {
		t.Fatalf("HasExistingState() after write error = %v", err)
	}
	if !hasState {
		t.Fatal("in-memory stores did not expose written state")
	}
}

func TestDisabledSnapshotStoreFailsInsteadOfCompactingLogs(t *testing.T) {
	store := disabledSnapshotStore{}
	_, err := store.Create(raftpkg.SnapshotVersionMax, 1, 1, raftpkg.Configuration{}, 1, nil)
	if !errors.Is(err, errRaftSnapshotsDisabled) {
		t.Fatalf("Create() error = %v, want %v", err, errRaftSnapshotsDisabled)
	}
}

func TestRaftStateSurvivesRestart(t *testing.T) {
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("initial ShutdownRaft() error = %v", err)
	}
	t.Cleanup(func() {
		if err := ShutdownRaft(); err != nil {
			t.Errorf("cleanup ShutdownRaft() error = %v", err)
		}
	})
	ResetMemDB()

	config := newTestRaftConfig(t, t.TempDir())
	config.RaftAddr = unusedTCPAddress(t)
	firstContext, cancelFirst := context.WithCancel(context.Background())
	if err := StartRaft(firstContext, config); err != nil {
		cancelFirst()
		t.Fatalf("first StartRaft() error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)

	command := &Cmd{Op: "SET", Args: []string{"durable-key", "durable-value"}}
	if got := ApplyCmdViaRaft(command, 3*time.Second); got != "+OK\r\n" {
		t.Fatalf("ApplyCmdViaRaft() = %q, want OK", got)
	}

	raftNodeMu.RLock()
	firstNode := raftNode
	commandIndex, expectedLog := findCommandLog(t, firstNode.stores.logStore, command)
	termBefore := getStableUint64(t, firstNode.stores.stableStore, currentTermKey)
	voteTermBefore := getStableUint64(t, firstNode.stores.stableStore, lastVoteTermKey)
	voteCandidateBefore := getStableBytes(t, firstNode.stores.stableStore, lastVoteCandKey)
	raftNodeMu.RUnlock()
	if termBefore == 0 || voteTermBefore == 0 || len(voteCandidateBefore) == 0 {
		t.Fatalf("elected node did not persist term/vote: term=%d voteTerm=%d candidate=%q", termBefore, voteTermBefore, voteCandidateBefore)
	}

	cancelFirst()
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("first ShutdownRaft() error = %v", err)
	}

	stored, err := newPersistentRaftStores(config)
	if err != nil {
		t.Fatalf("open persisted stores error = %v", err)
	}
	hasState, err := raftpkg.HasExistingState(stored.logStore, stored.stableStore, stored.snapshotStore)
	if err != nil {
		t.Fatalf("HasExistingState() error = %v", err)
	}
	if !hasState {
		t.Fatal("persisted stores were treated as fresh")
	}
	if got := getStableUint64(t, stored.stableStore, currentTermKey); got != termBefore {
		t.Fatalf("current term after reopen = %d, want %d", got, termBefore)
	}
	if got := getStableUint64(t, stored.stableStore, lastVoteTermKey); got != voteTermBefore {
		t.Fatalf("last vote term after reopen = %d, want %d", got, voteTermBefore)
	}
	if got := getStableBytes(t, stored.stableStore, lastVoteCandKey); !bytes.Equal(got, voteCandidateBefore) {
		t.Fatalf("last vote candidate after reopen = %q, want %q", got, voteCandidateBefore)
	}
	var persistedLog raftpkg.Log
	if err := stored.logStore.GetLog(commandIndex, &persistedLog); err != nil {
		t.Fatalf("GetLog(%d) after reopen error = %v", commandIndex, err)
	}
	if persistedLog.Type != expectedLog.Type || !bytes.Equal(persistedLog.Data, expectedLog.Data) {
		t.Fatalf("persisted log = %#v, want type=%v data=%q", persistedLog, expectedLog.Type, expectedLog.Data)
	}
	if err := stored.Close(); err != nil {
		t.Fatalf("persisted stores Close() error = %v", err)
	}

	// An initialized node must not try to join or bootstrap again. Keeping an
	// unreachable JoinAddr here makes an accidental rejoin fail the test.
	config.JoinAddr = "127.0.0.1:1"
	secondContext, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	if err := StartRaft(secondContext, config); err != nil {
		t.Fatalf("restart StartRaft() error = %v", err)
	}

	raftNodeMu.RLock()
	restartedNode := raftNode
	if !restartedNode.hadExistingState {
		raftNodeMu.RUnlock()
		t.Fatal("restarted node did not detect existing state")
	}
	configurationFuture := restartedNode.raft.GetConfiguration()
	raftNodeMu.RUnlock()
	if err := configurationFuture.Error(); err != nil {
		t.Fatalf("GetConfiguration() after restart error = %v", err)
	}
	servers := configurationFuture.Configuration().Servers
	if len(servers) != 1 || servers[0].ID != raftpkg.ServerID(config.GetRaftId()) {
		t.Fatalf("configuration after restart = %#v, want node %q", servers, config.GetRaftId())
	}
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("second ShutdownRaft() error = %v", err)
	}

	// Closing the restarted node must release both the TCP listener and the DB
	// lock, allowing a clean third open of the durable store.
	finalStores, err := newPersistentRaftStores(config)
	if err != nil {
		t.Fatalf("final store reopen error = %v", err)
	}
	if err := finalStores.Close(); err != nil {
		t.Fatalf("final stores Close() error = %v", err)
	}
}

func newTestRaftConfig(t *testing.T, dataDir string) *Config {
	t.Helper()
	return &Config{
		EnableRaft: true,
		raftID:     "test-node",
		RaftAddr:   "127.0.0.1:0",
		dataDir:    dataDir,
	}
}

func unusedTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("listener.Close() error = %v", err)
	}
	return address
}

func waitForRaftLeader(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if IsLeader() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("raft node did not become leader before timeout")
}

func findCommandLog(t *testing.T, store raftpkg.LogStore, command *Cmd) (uint64, raftpkg.Log) {
	t.Helper()
	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex() error = %v", err)
	}
	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex() error = %v", err)
	}
	want, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for index := first; index <= last; index++ {
		var entry raftpkg.Log
		if err := store.GetLog(index, &entry); err != nil {
			t.Fatalf("GetLog(%d) error = %v", index, err)
		}
		if entry.Type == raftpkg.LogCommand && bytes.Equal(entry.Data, want) {
			return index, entry
		}
	}
	t.Fatalf("command log %q not found between indexes %d and %d", want, first, last)
	return 0, raftpkg.Log{}
}

func getStableUint64(t *testing.T, store raftpkg.StableStore, key []byte) uint64 {
	t.Helper()
	value, err := store.GetUint64(key)
	if err != nil {
		t.Fatalf("GetUint64(%q) error = %v", key, err)
	}
	return value
}

func getStableBytes(t *testing.T, store raftpkg.StableStore, key []byte) []byte {
	t.Helper()
	value, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	return append([]byte(nil), value...)
}

func assertPermissions(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("permissions for %q = %04o, want %04o", path, got, want)
	}
}
