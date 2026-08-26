package core

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	raftpkg "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	bbolt "go.etcd.io/bbolt"
)

const (
	raftDirectoryMode  = 0o700
	raftDatabaseMode   = 0o600
	raftStoreOpenDelay = time.Second
)

var errRaftSnapshotsDisabled = errors.New("durable raft snapshots are not implemented")

// raftStores groups the storage interfaces Raft requires and owns their
// lifecycle. Production uses a single bbolt-backed store for both the log and
// stable state; tests can inject in-memory implementations explicitly.
type raftStores struct {
	logStore      raftpkg.LogStore
	stableStore   raftpkg.StableStore
	snapshotStore raftpkg.SnapshotStore
	closer        io.Closer
}

func (s *raftStores) Close() error {
	if s == nil || s.closer == nil {
		return nil
	}
	return s.closer.Close()
}

func newPersistentRaftStores(config *Config) (*raftStores, error) {
	nodeDir, err := raftNodeDataDir(config)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(nodeDir, raftDirectoryMode); err != nil {
		return nil, fmt.Errorf("create raft data directory %q: %w", nodeDir, err)
	}
	if err := os.Chmod(nodeDir, raftDirectoryMode); err != nil {
		return nil, fmt.Errorf("set raft data directory permissions on %q: %w", nodeDir, err)
	}

	dbPath := filepath.Join(nodeDir, "raft.db")
	store, err := raftboltdb.New(raftboltdb.Options{
		Path: dbPath,
		BoltOptions: &bbolt.Options{
			Timeout: raftStoreOpenDelay,
		},
		NoSync:                  false,
		MsgpackUseNewTimeFormat: true,
	})
	if err != nil {
		if errors.Is(err, bbolt.ErrTimeout) {
			return nil, fmt.Errorf("raft store %q is already in use: %w", dbPath, err)
		}
		return nil, fmt.Errorf("open raft store %q: %w", dbPath, err)
	}
	if err := os.Chmod(dbPath, raftDatabaseMode); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("set raft database permissions on %q: %w", dbPath, err)
	}

	return &raftStores{
		logStore:      store,
		stableStore:   store,
		snapshotStore: disabledSnapshotStore{},
		closer:        store,
	}, nil
}

func newInmemRaftStores() *raftStores {
	store := raftpkg.NewInmemStore()
	return &raftStores{
		logStore:      store,
		stableStore:   store,
		snapshotStore: raftpkg.NewInmemSnapshotStore(),
	}
}

func raftNodeDataDir(config *Config) (string, error) {
	if config == nil {
		return "", errors.New("raft config is required")
	}
	nodeID := config.GetRaftId()
	if nodeID == "" {
		return "", errors.New("--raft-id is required when Raft is enabled")
	}
	if nodeID == "." || nodeID == ".." || filepath.Base(nodeID) != nodeID || strings.Contains(nodeID, "\\") {
		return "", fmt.Errorf("invalid --raft-id %q: path separators are not allowed", nodeID)
	}
	return filepath.Join(config.GetDataDir(), "raft", nodeID), nil
}

// disabledSnapshotStore makes the issue #3 boundary explicit. A discard
// snapshot store reports success and lets Raft compact durable logs even though
// no restartable snapshot exists. Issue #4 will replace this implementation
// with a file-backed snapshot store.
type disabledSnapshotStore struct{}

func (disabledSnapshotStore) Create(
	raftpkg.SnapshotVersion,
	uint64,
	uint64,
	raftpkg.Configuration,
	uint64,
	raftpkg.Transport,
) (raftpkg.SnapshotSink, error) {
	return nil, errRaftSnapshotsDisabled
}

func (disabledSnapshotStore) List() ([]*raftpkg.SnapshotMeta, error) {
	return nil, nil
}

func (disabledSnapshotStore) Open(string) (*raftpkg.SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, errRaftSnapshotsDisabled
}
