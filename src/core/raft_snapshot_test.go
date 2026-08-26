package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"skiffdb/src/resp"

	raftpkg "github.com/hashicorp/raft"
)

func TestFSMSnapshotSerializationDoesNotBlockWrites(t *testing.T) {
	var db MemDB
	db.Init()
	db.Set("captured", "before")

	snapshot, err := (&fsm{db: &db}).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	defer snapshot.Release()

	sink := newBlockingSnapshotSink()
	persisted := make(chan error, 1)
	go func() {
		persisted <- snapshot.Persist(sink)
	}()
	select {
	case <-sink.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("snapshot serialization did not start")
	}

	written := make(chan struct{})
	go func() {
		db.Set("concurrent", "after")
		close(written)
	}()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("write remained blocked for the snapshot serialization period")
	}

	close(sink.allowWrite)
	if err := <-persisted; err != nil {
		t.Fatalf("Persist() error = %v", err)
	}

	var payload fsmSnapshotPayload
	if err := json.Unmarshal(sink.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(snapshot) error = %v", err)
	}
	if payload.Format != fsmSnapshotFormat || payload.Version != fsmSnapshotVersion {
		t.Fatalf("snapshot metadata = format %q version %d", payload.Format, payload.Version)
	}
	if got := payload.Keyspace["captured"]; got != "before" {
		t.Fatalf("captured value = %q, want before", got)
	}
	if _, ok := payload.Keyspace["concurrent"]; ok {
		t.Fatal("snapshot included a write performed after its point-in-time copy")
	}
}

func TestFSMRestoreRejectsUnknownSnapshotVersionWithoutChangingState(t *testing.T) {
	var db MemDB
	db.Init()
	db.Set("existing", "value")
	payload := fmt.Sprintf(`{"format":%q,"version":%d,"keyspace":{"replacement":"value"}}`, fsmSnapshotFormat, fsmSnapshotVersion+1)

	err := (&fsm{db: &db}).Restore(io.NopCloser(strings.NewReader(payload)))
	if err == nil || !strings.Contains(err.Error(), "unsupported FSM snapshot version") {
		t.Fatalf("Restore() error = %v, want unsupported-version error", err)
	}
	if got := db.Get("existing"); got == nil || *got != "value" {
		t.Fatalf("existing state changed after rejected restore: %v", got)
	}
	if got := db.Get("replacement"); got != nil {
		t.Fatalf("replacement state applied after rejected restore: %q", *got)
	}
}

func TestRaftSnapshotRestoresCapturedStateAndReplaysLogTail(t *testing.T) {
	config, cancel := startSingleTestRaft(t)

	applyTestCommand(t, &Cmd{Op: "SET", Args: []string{"snapshot-key", "captured"}})
	if got := ExecuteLocally(&Cmd{Op: "SAVE"}); got != resp.Ok {
		t.Fatalf("first SAVE response = %q, want %q", got, resp.Ok)
	}

	raftNodeMu.RLock()
	snapshots, err := raftNode.stores.snapshotStore.List()
	raftNodeMu.RUnlock()
	if err != nil {
		t.Fatalf("List() after SAVE error = %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("snapshot count after SAVE = %d, want 1", len(snapshots))
	}
	snapshotIndex := snapshots[0].Index

	// Make the configured snapshot path unusable so the next Raft snapshot
	// request reaches a real sink-creation failure. SAVE must surface it rather
	// than returning a false success.
	nodeDir, _ := raftNodeDataDir(config)
	snapshotDir := filepath.Join(nodeDir, raftSnapshotDirectory)
	movedSnapshotDir := snapshotDir + ".saved"
	if err := os.Rename(snapshotDir, movedSnapshotDir); err != nil {
		t.Fatalf("Rename(snapshot directory) error = %v", err)
	}
	if err := os.WriteFile(snapshotDir, []byte("not a directory"), raftDatabaseMode); err != nil {
		t.Fatalf("WriteFile(blocking snapshot path) error = %v", err)
	}
	if got := ExecuteLocally(&Cmd{Op: "SAVE"}); got == resp.Ok || !strings.HasPrefix(got, "-ERROR Snapshot failed:") {
		t.Fatalf("SAVE with unusable snapshot path response = %q, want a snapshot failure", got)
	}
	if err := os.Remove(snapshotDir); err != nil {
		t.Fatalf("Remove(blocking snapshot path) error = %v", err)
	}
	if err := os.Rename(movedSnapshotDir, snapshotDir); err != nil {
		t.Fatalf("restore snapshot directory error = %v", err)
	}

	tailCommand := &Cmd{Op: "SET", Args: []string{"tail-key", "replayed"}}
	applyTestCommand(t, tailCommand)
	raftNodeMu.RLock()
	tailIndex, _ := findCommandLog(t, raftNode.stores.logStore, tailCommand)
	snapshotPayload := readFSMSnapshot(t, raftNode.stores.snapshotStore, snapshots[0].ID)
	raftNodeMu.RUnlock()
	if tailIndex <= snapshotIndex {
		t.Fatalf("tail command index = %d, want greater than snapshot index %d", tailIndex, snapshotIndex)
	}
	if got := snapshotPayload.Keyspace["snapshot-key"]; got != "captured" {
		t.Fatalf("snapshot keyspace value = %q, want captured", got)
	}
	if _, ok := snapshotPayload.Keyspace["tail-key"]; ok {
		t.Fatal("snapshot contains a command committed after its captured log index")
	}

	cancel()
	restartSingleTestRaft(t, config)
	waitForMemDBValue(t, "snapshot-key", "captured", 6*time.Second)
	waitForMemDBValue(t, "tail-key", "replayed", 6*time.Second)
}

func TestRaftRestoreFallsBackFromCorruptLatestSnapshot(t *testing.T) {
	config, cancel := startSingleTestRaft(t)

	applyTestCommand(t, &Cmd{Op: "SET", Args: []string{"older", "snapshot"}})
	if err := SaveSnapshot(); err != nil {
		t.Fatalf("first SaveSnapshot() error = %v", err)
	}
	applyTestCommand(t, &Cmd{Op: "SET", Args: []string{"newer", "tail"}})
	if err := SaveSnapshot(); err != nil {
		t.Fatalf("second SaveSnapshot() error = %v", err)
	}

	raftNodeMu.RLock()
	snapshots, err := raftNode.stores.snapshotStore.List()
	raftNodeMu.RUnlock()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(snapshots) < 2 {
		t.Fatalf("snapshot count = %d, want at least 2", len(snapshots))
	}
	latestID := snapshots[0].ID

	cancel()
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("ShutdownRaft() error = %v", err)
	}
	nodeDir, _ := raftNodeDataDir(config)
	statePath := filepath.Join(nodeDir, raftSnapshotDirectory, latestID, "state.bin")
	stateFile, err := os.OpenFile(statePath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile(%q) error = %v", statePath, err)
	}
	if _, err := stateFile.WriteAt([]byte("corrupt"), 0); err != nil {
		_ = stateFile.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := stateFile.Close(); err != nil {
		t.Fatalf("state file Close() error = %v", err)
	}

	ResetMemDB()
	config.JoinAddr = "127.0.0.1:1"
	config.RaftAddr = unusedTCPAddress(t)
	restartContext, cancelRestart := context.WithCancel(context.Background())
	t.Cleanup(cancelRestart)
	if err := StartRaft(restartContext, config); err != nil {
		t.Fatalf("StartRaft() with corrupt latest snapshot error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)
	waitForMemDBValue(t, "older", "snapshot", 6*time.Second)
	waitForMemDBValue(t, "newer", "tail", 6*time.Second)
}

func TestRaftRestoreIgnoresIncompleteLatestSnapshot(t *testing.T) {
	config, cancel := startSingleTestRaft(t)

	applyTestCommand(t, &Cmd{Op: "SET", Args: []string{"captured", "snapshot"}})
	if err := SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	applyTestCommand(t, &Cmd{Op: "SET", Args: []string{"after", "tail"}})
	cancel()
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("ShutdownRaft() error = %v", err)
	}

	nodeDir, _ := raftNodeDataDir(config)
	incompleteDir := filepath.Join(
		nodeDir,
		raftSnapshotDirectory,
		"999-999-999"+raftIncompleteSnapshotSuffix,
	)
	if err := os.MkdirAll(incompleteDir, raftDirectoryMode); err != nil {
		t.Fatalf("MkdirAll(incomplete snapshot) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(incompleteDir, "meta.json"), []byte(`{"Version":1,"Index":999,"Term":999}`), raftDatabaseMode); err != nil {
		t.Fatalf("WriteFile(incomplete metadata) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(incompleteDir, "state.bin"), []byte("partial"), raftDatabaseMode); err != nil {
		t.Fatalf("WriteFile(incomplete state) error = %v", err)
	}

	ResetMemDB()
	config.JoinAddr = "127.0.0.1:1"
	config.RaftAddr = unusedTCPAddress(t)
	restartContext, cancelRestart := context.WithCancel(context.Background())
	t.Cleanup(cancelRestart)
	if err := StartRaft(restartContext, config); err != nil {
		t.Fatalf("StartRaft() with incomplete latest snapshot error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)
	waitForMemDBValue(t, "captured", "snapshot", 6*time.Second)
	waitForMemDBValue(t, "after", "tail", 6*time.Second)
}

func startSingleTestRaft(t *testing.T) (*Config, context.CancelFunc) {
	t.Helper()
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
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := StartRaft(ctx, config); err != nil {
		cancel()
		t.Fatalf("StartRaft() error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)
	return config, cancel
}

func restartSingleTestRaft(t *testing.T, config *Config) {
	t.Helper()
	if err := ShutdownRaft(); err != nil {
		t.Fatalf("ShutdownRaft() error = %v", err)
	}
	ResetMemDB()
	config.JoinAddr = "127.0.0.1:1"
	config.RaftAddr = unusedTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := StartRaft(ctx, config); err != nil {
		cancel()
		t.Fatalf("restart StartRaft() error = %v", err)
	}
	waitForRaftLeader(t, 6*time.Second)
}

func applyTestCommand(t *testing.T, command *Cmd) {
	t.Helper()
	if got := ApplyCmdViaRaft(command, 3*time.Second); got != resp.Ok {
		t.Fatalf("ApplyCmdViaRaft(%s) = %q, want %q", command.ToString(), got, resp.Ok)
	}
}

func waitForMemDBValue(t *testing.T, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := memdb.Get(key); got != nil && *got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := memdb.Get(key)
	if got == nil {
		t.Fatalf("key %q was not restored before timeout", key)
	}
	t.Fatalf("key %q = %q, want %q", key, *got, want)
}

func readFSMSnapshot(t *testing.T, store raftpkg.SnapshotStore, id string) fsmSnapshotPayload {
	t.Helper()
	_, reader, err := store.Open(id)
	if err != nil {
		t.Fatalf("Open(snapshot %q) error = %v", id, err)
	}
	defer reader.Close()
	var payload fsmSnapshotPayload
	if err := json.NewDecoder(reader).Decode(&payload); err != nil {
		t.Fatalf("Decode(snapshot %q) error = %v", id, err)
	}
	return payload
}

type blockingSnapshotSink struct {
	bytes.Buffer
	writeStarted chan struct{}
	allowWrite   chan struct{}
	startOnce    sync.Once
}

func newBlockingSnapshotSink() *blockingSnapshotSink {
	return &blockingSnapshotSink{
		writeStarted: make(chan struct{}),
		allowWrite:   make(chan struct{}),
	}
}

func (s *blockingSnapshotSink) ID() string { return "blocking-test-snapshot" }

func (s *blockingSnapshotSink) Write(data []byte) (int, error) {
	s.startOnce.Do(func() { close(s.writeStarted) })
	<-s.allowWrite
	return s.Buffer.Write(data)
}

func (s *blockingSnapshotSink) Close() error  { return nil }
func (s *blockingSnapshotSink) Cancel() error { return nil }

var _ raftpkg.SnapshotSink = (*blockingSnapshotSink)(nil)
