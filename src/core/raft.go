package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"skiffdb/proto/cluster"
	"skiffdb/src/resp"

	// "github.com/fanyi-zhao/skiffdb/src/core" // Removed to fix import cycle

	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftpkg "github.com/hashicorp/raft"
)

// RaftNode encapsulates the HashiCorp Raft instance for this process.
type RaftNode struct {
	raft             *raftpkg.Raft
	localID          *raftpkg.ServerID
	transport        *raftpkg.NetworkTransport
	stores           *raftStores
	hadExistingState bool
	closeOnce        sync.Once
	closeErr         error
}

var (
	raftNode   *RaftNode
	raftNodeMu sync.RWMutex
)

// ClusterRole represents the suffrage for a cluster member.
type ClusterRole int

const (
	ClusterRoleUnspecified ClusterRole = iota
	ClusterRoleVoter
	ClusterRoleNonVoter
)

// ClusterMember represents a single node in the cluster configuration.
type ClusterMember struct {
	NodeID string
	Addr   string
	Role   ClusterRole
}

// ClusterInfo captures the leader and members for introspection responses.
type ClusterInfo struct {
	LeaderID        string
	LeaderAddr      string
	LeaderAdminAddr string
	Members         []ClusterMember
}

// ErrRaftNotEnabled indicates Raft was never started on this node.
var ErrRaftNotEnabled = errors.New("raft not enabled")

// NotLeaderError is returned when a leadership-sensitive call is routed to a follower.
type NotLeaderError struct {
	LeaderAdminAddr string
}

const (
	OpLeaderChanged       = "LEADER_CHANGED"
	raftSnapshotThreshold = uint64(8192)
	raftSnapshotInterval  = 2 * time.Minute
	// Keep enough log tail to replay from the oldest regularly spaced retained
	// snapshot if newer snapshots fail validation during restart.
	raftSnapshotTrailingLogs = raftSnapshotThreshold * uint64(raftSnapshotRetention-1)
	fsmSnapshotFormat        = "skiffdb-fsm"
	fsmSnapshotVersion       = uint64(1)
)

var leaderAdminAddr = ""

func GetLeaderAdminAddr() string {
	return leaderAdminAddr
}

func (e *NotLeaderError) Error() string {
	if e == nil {
		return "not leader"
	}
	if e.LeaderAdminAddr == "" {
		return "not leader"
	}
	return fmt.Sprintf("not leader (adminAddr=%s)", e.LeaderAdminAddr)
}

// RaftEnabled returns true if a raft node has been started.
func RaftEnabled() bool {
	raftNodeMu.RLock()
	defer raftNodeMu.RUnlock()
	return raftNode != nil && raftNode.raft != nil
}

// IsLeader returns true if this node is the current leader.
func IsLeader() bool {
	raftNodeMu.RLock()
	defer raftNodeMu.RUnlock()
	if raftNode == nil || raftNode.raft == nil {
		return false
	}
	return raftNode.raft.State() == raftpkg.Leader
}

// StartRaft starts a Raft node backed by durable local storage.
func StartRaft(ctx context.Context, config *Config) error {
	if config == nil {
		return errors.New("raft config is required")
	}

	raftNodeMu.Lock()
	defer raftNodeMu.Unlock()
	if raftNode != nil {
		return fmt.Errorf("duplicated call to StartRaft is not permitted")
	}

	if !config.EnableRaft {
		log.Default().Printf("config.EnableRaft is false, ignored")
		return nil
	}

	stores, err := newPersistentRaftStores(config)
	if err != nil {
		return fmt.Errorf("initialize durable raft storage: %w", err)
	}

	node, err := initRaftNode(config, stores)
	if err != nil {
		_ = stores.Close()
		return err
	}

	if !node.hadExistingState {
		if config.JoinAddr == "" {
			err = createNewCluster(node, config)
		} else {
			err = joinCluster(ctx, config)
		}
		if err != nil {
			closeErr := node.Close()
			return errors.Join(err, closeErr)
		}
	} else {
		log.Printf("Reopened existing Raft state for node %s", config.GetRaftId())
	}

	raftNode = node
	// observe cluster state changes
	observeClusterLeaderChange(ctx, node)
	return nil
}

// ShutdownRaft stops Raft before closing its transport and durable stores.
func ShutdownRaft() error {
	raftNodeMu.Lock()
	defer raftNodeMu.Unlock()
	if raftNode == nil {
		return nil
	}
	node := raftNode
	raftNode = nil
	return node.Close()
}

func (node *RaftNode) Close() error {
	if node == nil {
		return nil
	}
	node.closeOnce.Do(func() {
		var closeErrors []error
		if node.raft != nil {
			if err := node.raft.Shutdown().Error(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("shutdown raft: %w", err))
			}
		}
		if node.transport != nil {
			if err := node.transport.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close raft transport: %w", err))
			}
		}
		if node.stores != nil {
			if err := node.stores.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close raft stores: %w", err))
			}
		}
		node.closeErr = errors.Join(closeErrors...)
	})
	return node.closeErr
}

var ErrInternal = errors.New("failed to join the cluster: internal error")

func createNewCluster(node *RaftNode, config *Config) error {
	confFuture := node.raft.BootstrapCluster(raftpkg.Configuration{Servers: []raftpkg.Server{
		{
			Suffrage: raftpkg.Voter,
			ID:       *node.localID,
			Address:  raftpkg.ServerAddress(config.RaftAddr),
		},
	}})
	if err := confFuture.Error(); err != nil {
		return fmt.Errorf("bootstrap raft cluster: %w", err)
	}
	return nil
}

func joinCluster(ctx context.Context, config *Config) error {
	sendRequest := func() error {
		// init connection
		log.Printf("Prepare to join cluster. Connecting to: %v ...", config.JoinAddr)
		conn, err := grpc.NewClient(config.JoinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect cluster: %s. %v", config.JoinAddr, err)
			return ErrInternal
		}
		defer conn.Close()

		// send request
		client := cluster.NewClusterAdminClient(conn)
		log.Println("Sending JoinClusterRequest.")
		response, err := client.JoinCluster(ctx, &cluster.JoinClusterRequest{
			NodeId: config.GetRaftId(),
			Addr:   config.RaftAddr,
		})

		// check error
		if err != nil {
			log.Printf("Failed to join the cluster: %s. %v", config.JoinAddr, err)
			return ErrInternal
		}

		// check response
		switch response.Code {
		case cluster.JoinClusterResponseCode_SUCCESS:
			return nil
		case cluster.JoinClusterResponseCode_RAFT_NOT_ENABLED:
			return retry.Unrecoverable(fmt.Errorf("fatal status: %v is not a raft node", config.JoinAddr))
		case cluster.JoinClusterResponseCode_NOT_LEADER:
			if response.Cluster != nil {
				return &NotLeaderError{
					LeaderAdminAddr: response.Cluster.LeaderAdminAddr,
				}
			}
			return ErrInternal
		default:
			log.Printf("Failed to join the cluster for unknown reason")
			return ErrInternal
		}
	}

	err := retry.Do(
		sendRequest,
		retry.Attempts(5),
		retry.Delay(500*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
		retry.Context(ctx),
		retry.OnRetry(func(n uint, err error) {
			log.Printf("attempt %d, reason:%v\n", n+1, err)
			if nle, ok := err.(*NotLeaderError); ok {
				log.Printf("Cluster leader changed from %s to %s. Will update the JoinAddr and try again.", config.JoinAddr, nle.LeaderAdminAddr)
				config.JoinAddr = nle.LeaderAdminAddr
			}
		}),
		retry.LastErrorOnly(true),
	)
	if err == nil {
		log.Printf("Successfully joined the cluster. Leader: %v", config.JoinAddr)
	}
	return err
}

func initRaftNode(config *Config, stores *raftStores) (*RaftNode, error) {
	if stores == nil || stores.logStore == nil || stores.stableStore == nil || stores.snapshotStore == nil {
		return nil, errors.New("raft stores are required")
	}
	conf := raftpkg.DefaultConfig()
	conf.LocalID = raftpkg.ServerID(config.GetRaftId())
	conf.SnapshotThreshold = raftSnapshotThreshold
	conf.SnapshotInterval = raftSnapshotInterval
	conf.TrailingLogs = raftSnapshotTrailingLogs

	hadExistingState, err := raftpkg.HasExistingState(stores.logStore, stores.stableStore, stores.snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("inspect existing raft state: %w", err)
	}

	transport, err := raftpkg.NewTCPTransport(config.RaftAddr, nil, 3, 10*time.Second, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP transport: %w", err)
	}

	fsm := &fsm{db: &memdb}

	r, err := raftpkg.NewRaft(conf, fsm, stores.logStore, stores.stableStore, stores.snapshotStore, transport)
	if err != nil {
		_ = transport.Close()
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	id := raftpkg.ServerID(config.GetRaftId())
	return &RaftNode{
		raft:             r,
		localID:          &id,
		transport:        transport,
		stores:           stores,
		hadExistingState: hadExistingState,
	}, nil
}

func observeClusterLeaderChange(ctx context.Context, node *RaftNode) {
	obsCh := make(chan raftpkg.Observation, 64)
	observer := raftpkg.NewObserver(obsCh, false, func(o *raftpkg.Observation) bool {
		_, ok := o.Data.(raftpkg.LeaderObservation)
		return ok
	})
	node.raft.RegisterObserver(observer)
	workCh := make(chan raftpkg.LeaderObservation, 64)
	go func() {
		defer node.raft.DeregisterObserver(observer)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-obsCh:
				if !ok {
					return
				}
				leaderEvent, ok := msg.Data.(raftpkg.LeaderObservation)
				if !ok {
					continue
				}
				if leaderEvent.LeaderID == *node.localID {
					// Notify all followers of my gRPC address
					workCh <- leaderEvent
				}
				// default:
				// log.Printf("leader event dropped: queue full")
			}
		}
	}()
	go func() {
		var lastLeader raftpkg.ServerID

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-workCh:
				if !ok {
					return
				}

				if ev.LeaderID == lastLeader {
					continue
				}
				lastLeader = ev.LeaderID

				applyCmdViaRaftNode(node, &Cmd{
					Op:   OpLeaderChanged,
					Args: []string{GetLeaderAdminAddr()},
				}, 3*time.Second)
			}
		}
	}()
}

// JoinCluster registers a new node with the running raft cluster.
func JoinCluster(nodeID string, addr string) (*ClusterInfo, error) {
	raftNodeMu.RLock()
	defer raftNodeMu.RUnlock()
	if raftNode == nil || raftNode.raft == nil {
		return nil, ErrRaftNotEnabled
	}
	if raftNode.raft.State() != raftpkg.Leader {
		return nil, &NotLeaderError{
			LeaderAdminAddr: leaderAdminAddr,
		}
	}
	if strings.TrimSpace(nodeID) == "" || strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("node id and address are required")
	}
	future := raftNode.raft.AddNonvoter(raftpkg.ServerID(nodeID), raftpkg.ServerAddress(addr), 0, 30*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("add nonvoter: %w", err)
	}
	return buildClusterInfoForNode(raftNode)
}

// buildClusterInfo materialises the current configuration for RPC responses.
func buildClusterInfo() (*ClusterInfo, error) {
	raftNodeMu.RLock()
	defer raftNodeMu.RUnlock()
	if raftNode == nil || raftNode.raft == nil {
		return nil, ErrRaftNotEnabled
	}
	return buildClusterInfoForNode(raftNode)
}

func buildClusterInfoForNode(node *RaftNode) (*ClusterInfo, error) {
	cfgFuture := node.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return nil, fmt.Errorf("get configuration: %w", err)
	}
	config := cfgFuture.Configuration()
	leaderAddr, leaderID := node.raft.LeaderWithID()
	info := &ClusterInfo{
		LeaderID:        string(leaderID),
		LeaderAddr:      string(leaderAddr),
		LeaderAdminAddr: leaderAdminAddr,
	}
	info.Members = make([]ClusterMember, 0, len(config.Servers))
	for _, srv := range config.Servers {
		info.Members = append(info.Members, ClusterMember{
			NodeID: string(srv.ID),
			Addr:   string(srv.Address),
			Role:   roleFromSuffrage(srv.Suffrage),
		})
	}
	return info, nil
}

func roleFromSuffrage(s raftpkg.ServerSuffrage) ClusterRole {
	switch s {
	case raftpkg.Voter:
		return ClusterRoleVoter
	case raftpkg.Nonvoter, raftpkg.Staging:
		return ClusterRoleNonVoter
	default:
		return ClusterRoleUnspecified
	}
}

// ApplyCmdViaRaft replicates an operation through Raft and waits for it to apply.
func ApplyCmdViaRaft(cmd *Cmd, timeout time.Duration) string {
	raftNodeMu.RLock()
	defer raftNodeMu.RUnlock()
	if raftNode == nil || raftNode.raft == nil {
		// Fallback: apply locally when raft disabled
		return ExecuteLocally(cmd)
	}
	return applyCmdViaRaftNode(raftNode, cmd, timeout)
}

func applyCmdViaRaftNode(node *RaftNode, cmd *Cmd, timeout time.Duration) string {
	payload, err := json.Marshal(cmd)
	if err != nil {
		return resp.ErrInternal
	}
	f := node.raft.Apply(payload, timeout)
	if err := f.Error(); err != nil {
		return resp.ErrInternal
	}
	respVal := f.Response()
	if respVal == nil {
		return resp.Ok
	}
	if errResp, ok := respVal.(error); ok {
		log.Printf("raft apply: fsm returned error: %v", errResp)
		return resp.ErrInternal
	}
	if msg, ok := respVal.(string); ok {
		return msg
	}
	log.Printf("raft apply: unexpected response type %T", respVal)
	return resp.ErrInternal
}

// SaveSnapshot requests a local Raft snapshot and waits until the snapshot is
// durable and Raft has completed its post-snapshot log compaction. When Raft is
// disabled, SAVE retains its historical no-op behavior.
func SaveSnapshot() error {
	raftNodeMu.RLock()
	defer raftNodeMu.RUnlock()
	if raftNode == nil || raftNode.raft == nil {
		return nil
	}
	if err := raftNode.raft.Snapshot().Error(); err != nil {
		return fmt.Errorf("raft snapshot: %w", err)
	}
	return nil
}

// fsm implements raft.FSM, applying log entries to the in-memory DB.
type fsm struct {
	db *MemDB
}

// Apply decodes the operation and mutates state.
func (f *fsm) Apply(l *raftpkg.Log) interface{} {
	var cmd Cmd
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		log.Printf("fsm apply: decode error: %v", err)
		return err
	}
	switch cmd.Op {
	case OpLeaderChanged:
		leaderAdminAddr = cmd.Args[0]
		return resp.Ok
	default:
		return ExecuteLocally(&cmd)
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
	var snapshot fsmSnapshotPayload
	if err := dec.Decode(&snapshot); err != nil {
		return fmt.Errorf("decode FSM snapshot: %w", err)
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("decode FSM snapshot: unexpected trailing JSON value")
		}
		return fmt.Errorf("decode FSM snapshot trailing data: %w", err)
	}
	if snapshot.Format != fsmSnapshotFormat {
		return fmt.Errorf("unsupported FSM snapshot format %q", snapshot.Format)
	}
	if snapshot.Version != fsmSnapshotVersion {
		return fmt.Errorf("unsupported FSM snapshot version %d", snapshot.Version)
	}
	if snapshot.Keyspace == nil {
		return errors.New("FSM snapshot keyspace is missing")
	}
	f.db.mutex.Lock()
	defer f.db.mutex.Unlock()
	f.db.keyspace = snapshot.Keyspace
	return nil
}

type fsmSnapshotPayload struct {
	Format   string            `json:"format"`
	Version  uint64            `json:"version"`
	Keyspace map[string]string `json:"keyspace"`
}

// memSnapshot implements raft.FSMSnapshot
type memSnapshot struct {
	state map[string]string
}

func (m *memSnapshot) Persist(sink raftpkg.SnapshotSink) error {
	enc := json.NewEncoder(sink)
	payload := fsmSnapshotPayload{
		Format:   fsmSnapshotFormat,
		Version:  fsmSnapshotVersion,
		Keyspace: m.state,
	}
	if err := enc.Encode(&payload); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (m *memSnapshot) Release() {}
