package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	// "kvdb/src/core" // Removed to fix import cycle

	"kvdb/src/resp"

	"strings"
	"time"

	"kvdb/proto/cluster"

	"github.com/avast/retry-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftpkg "github.com/hashicorp/raft"
)

// RaftNode encapsulates the Hashicorp Raft instance for this process.
type RaftNode struct {
	raft    *raftpkg.Raft
	localID *raftpkg.ServerID
}

var raftNode *RaftNode

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
	OpLeaderChanged = "LEADER_CHANGED"
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
func RaftEnabled() bool { return raftNode != nil && raftNode.raft != nil }

// IsLeader returns true if this node is the current leader.
func IsLeader() bool {
	if !RaftEnabled() {
		return false
	}
	return raftNode.raft.State() == raftpkg.Leader
}

// StartRaft starts a minimal Raft node with in-memory stores.
func StartRaft(ctx context.Context, config *Config) error {
	if RaftEnabled() {
		return fmt.Errorf("duplicated call to StartRaft is not permitted")
	}

	if !config.EnableRaft {
		log.Default().Printf("config.EnableRaft is false, ignored")
		return nil
	}

	var err error
	raftNode, err = initRaftNode()
	if err != nil {
		log.Default().Fatalf("Failed to init Raft FSM.", err)
		return err
	}

	if config.JoinAddr == "" {
		createNewCluster(ctx)
	} else {
		err = joinCluster(ctx)
	}

	if err != nil {
		log.Default().Fatalf("Failed to init Raft FSM. Err:%v", err)
		return err
	}

	// observe cluster state changes
	observeClusterLeaderChange(ctx)
	return nil
}

var ErrInternal = errors.New("failed to join the cluster: internal error")

func createNewCluster(ctx context.Context) {
	confFuture := raftNode.raft.BootstrapCluster(raftpkg.Configuration{Servers: []raftpkg.Server{
		{
			Suffrage: raftpkg.Voter,
			ID:       *raftNode.localID,
			Address:  raftpkg.ServerAddress(DBConfig.RaftAddr),
		},
	}})
	if err := confFuture.Error(); err != nil && !strings.Contains(err.Error(), "bootstrap") {
		// If already bootstrapped, ignore; otherwise report.
		log.Printf("raft bootstrap warning: %v", err)
	}
}

func joinCluster(ctx context.Context) error {
	sendRequest := func() error {
		// init connection
		log.Printf("Prepare to join cluster. Connecting to: %v ...", DBConfig.JoinAddr)
		conn, err := grpc.NewClient(DBConfig.JoinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect cluster: %s. %v", DBConfig.JoinAddr, err)
			return ErrInternal
		}
		defer conn.Close()

		// send request
		client := cluster.NewClusterAdminClient(conn)
		log.Println("Sending JoinClusterRequest.")
		response, err := client.JoinCluster(ctx, &cluster.JoinClusterRequest{
			NodeId: DBConfig.GetRaftId(),
			Addr:   DBConfig.RaftAddr,
		})

		// check error
		if err != nil {
			log.Printf("Failed to join the cluster: %s. %v", DBConfig.JoinAddr, err)
			return ErrInternal
		}

		// check response
		switch response.Code {
		case cluster.JoinClusterResponseCode_SUCCESS:
			return nil
		case cluster.JoinClusterResponseCode_RAFT_NOT_ENABLED:
			return retry.Unrecoverable(fmt.Errorf("fatal status: %v is not a raft node", DBConfig.JoinAddr))
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
				log.Printf("Cluster leader changed from %s to %s. Will update the JoinAddr and try again.", DBConfig.JoinAddr, nle.LeaderAdminAddr)
				DBConfig.JoinAddr = nle.LeaderAdminAddr
			}
		}),
		retry.LastErrorOnly(true),
	)
	if err == nil {
		log.Printf("Successfully joined the cluster. Leader: %v", DBConfig.JoinAddr)
	}
	return err
}

func initRaftNode() (*RaftNode, error) {
	conf := raftpkg.DefaultConfig()
	conf.LocalID = raftpkg.ServerID(DBConfig.GetRaftId())

	transport, err := raftpkg.NewTCPTransport(DBConfig.RaftAddr, nil, 3, 10*time.Second, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP transport: %w", err)
	}

	stableStore := raftpkg.NewInmemStore()
	logStore := raftpkg.NewInmemStore()
	snapshotStore := raftpkg.NewDiscardSnapshotStore()

	fsm := &fsm{db: &memdb}

	r, err := raftpkg.NewRaft(conf, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	id := raftpkg.ServerID(DBConfig.GetRaftId())
	return &RaftNode{raft: r, localID: &id}, nil
}

func observeClusterLeaderChange(ctx context.Context) {
	obsCh := make(chan raftpkg.Observation, 64)
	observer := raftpkg.NewObserver(obsCh, false, func(o *raftpkg.Observation) bool {
		_, ok := o.Data.(raftpkg.LeaderObservation)
		return ok
	})
	raftNode.raft.RegisterObserver(observer)
	workCh := make(chan raftpkg.LeaderObservation, 64)
	go func() {
		defer raftNode.raft.DeregisterObserver(observer)
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
				if leaderEvent.LeaderID == *raftNode.localID {
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

				ApplyCmdViaRaft(&Cmd{
					Op:   OpLeaderChanged,
					Args: []string{GetLeaderAdminAddr()},
				}, 3*time.Second)
			}
		}
	}()
}

// JoinCluster registers a new node with the running raft cluster.
func JoinCluster(nodeID string, addr string) (*ClusterInfo, error) {
	if !RaftEnabled() {
		return nil, ErrRaftNotEnabled
	}
	if !IsLeader() {
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
	return buildClusterInfo()
}

// buildClusterInfo materialises the current configuration for RPC responses.
func buildClusterInfo() (*ClusterInfo, error) {
	if !RaftEnabled() {
		return nil, ErrRaftNotEnabled
	}
	cfgFuture := raftNode.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return nil, fmt.Errorf("get configuration: %w", err)
	}
	config := cfgFuture.Configuration()
	leaderAddr, leaderID := raftNode.raft.LeaderWithID()
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
	if !RaftEnabled() {
		// Fallback: apply locally when raft disabled
		return ExecuteLocally(cmd)
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return resp.ErrInternal
	}
	f := raftNode.raft.Apply(payload, timeout)
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
type memSnapshot struct {
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
