package core

import (
    "encoding/json"
    "fmt"
    "io"
    "log"
    "strings"
    "time"

    raftpkg "github.com/hashicorp/raft"
)

// Operation represents a replicated command in the Raft log.
type Operation struct {
    Op    string `json:"op"`
    Key   string `json:"key"`
    Value string `json:"value,omitempty"`
    Delta int    `json:"delta,omitempty"`
}

// RaftNode encapsulates the Hashicorp Raft instance for this process.
type RaftNode struct {
    raft *raftpkg.Raft
}

var raftNode *RaftNode

// RaftEnabled returns true if a raft node has been started.
func RaftEnabled() bool { return raftNode != nil && raftNode.raft != nil }

// IsLeader returns true if this node is the current leader.
func IsLeader() bool {
    if !RaftEnabled() {
        return false
    }
    return raftNode.raft.State() == raftpkg.Leader
}

// StartRaft starts a minimal Raft node with in-memory stores.
// peers is a map of serverID -> raftAddr (e.g., "node1" -> "127.0.0.1:7000").
// If peers is non-empty, the cluster will be bootstrapped with these peers.
func StartRaft(nodeID string, raftAddr string, peers map[string]string) error {
    if RaftEnabled() {
        return nil
    }

    conf := raftpkg.DefaultConfig()
    conf.LocalID = raftpkg.ServerID(nodeID)

    transport, err := raftpkg.NewTCPTransport(raftAddr, nil, 3, 10*time.Second, nil)
    if err != nil {
        return fmt.Errorf("failed to create TCP transport: %w", err)
    }

    // Minimal in-memory stores. For production, use persistent stores.
    stableStore := raftpkg.NewInmemStore()
    logStore := raftpkg.NewInmemStore()
    snapshotStore := raftpkg.NewDiscardSnapshotStore()

    fsm := &fsm{db: &memdb}

    r, err := raftpkg.NewRaft(conf, fsm, logStore, stableStore, snapshotStore, transport)
    if err != nil {
        return fmt.Errorf("failed to create raft: %w", err)
    }

    raftNode = &RaftNode{raft: r}

    if len(peers) > 0 {
        var servers []raftpkg.Server
        for id, addr := range peers {
            servers = append(servers, raftpkg.Server{
                Suffrage: raftpkg.Voter,
                ID:       raftpkg.ServerID(id),
                Address:  raftpkg.ServerAddress(addr),
            })
        }
        confFuture := r.BootstrapCluster(raftpkg.Configuration{Servers: servers})
        if err := confFuture.Error(); err != nil && !strings.Contains(err.Error(), "bootstrap") {
            // If already bootstrapped, ignore; otherwise report.
            log.Printf("raft bootstrap warning: %v", err)
        }
    }
    return nil
}

// ApplyOperation replicates an operation through Raft and waits for it to apply.
func ApplyOperation(op Operation, timeout time.Duration) (interface{}, error) {
    if !RaftEnabled() {
        // Fallback: apply locally when raft disabled
        return applyLocally(op)
    }
    payload, err := json.Marshal(op)
    if err != nil {
        return nil, err
    }
    f := raftNode.raft.Apply(payload, timeout)
    if err := f.Error(); err != nil {
        return nil, err
    }
    return f.Response(), nil
}

// applyLocally applies the operation directly to the local DB (no Raft).
func applyLocally(op Operation) (interface{}, error) {
    switch strings.ToUpper(op.Op) {
    case "SET":
        memdb.Set(op.Key, op.Value)
        return nil, nil
    case "DEL":
        memdb.Delete(op.Key)
        return nil, nil
    case "INCR":
        return nil, memdb.Incr(op.Key, op.Delta)
    default:
        return nil, fmt.Errorf("unsupported op: %s", op.Op)
    }
}

// fsm implements raft.FSM, applying log entries to the in-memory DB.
type fsm struct{
    db *MemDB
}

// Apply decodes the operation and mutates state.
func (f *fsm) Apply(l *raftpkg.Log) interface{} {
    var op Operation
    if err := json.Unmarshal(l.Data, &op); err != nil {
        log.Printf("fsm apply: decode error: %v", err)
        return err
    }
    switch strings.ToUpper(op.Op) {
    case "SET":
        f.db.Set(op.Key, op.Value)
        return nil
    case "DEL":
        f.db.Delete(op.Key)
        return nil
    case "INCR":
        if err := f.db.Incr(op.Key, op.Delta); err != nil {
            return err
        }
        return nil
    default:
        return fmt.Errorf("unsupported op: %s", op.Op)
    }
}

// Snapshot creates a point-in-time snapshot of the DB.
func (f *fsm) Snapshot() (raftpkg.FSMSnapshot, error) {
    // Simple snapshot of the keyspace map.
    f.db.mutex.RLock()
    defer f.db.mutex.RUnlock()
    cp := make(map[string]string, len(f.db.keyspace))
    for k, v := range f.db.keyspace {
        cp[k] = v
    }
    return &memSnapshot{state: cp}, nil
}

// Restore replaces the DB state from a snapshot.
func (f *fsm) Restore(rc io.ReadCloser) error {
    defer rc.Close()
    dec := json.NewDecoder(rc)
    data := make(map[string]string)
    if err := dec.Decode(&data); err != nil {
        return err
    }
    f.db.mutex.Lock()
    defer f.db.mutex.Unlock()
    f.db.keyspace = data
    return nil
}

// memSnapshot implements raft.FSMSnapshot
type memSnapshot struct{
    state map[string]string
}

func (m *memSnapshot) Persist(sink raftpkg.SnapshotSink) error {
    enc := json.NewEncoder(sink)
    if err := enc.Encode(m.state); err != nil {
        _ = sink.Cancel()
        return err
    }
    return sink.Close()
}

func (m *memSnapshot) Release() {}

