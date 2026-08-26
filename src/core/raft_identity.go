package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	raftIdentityFilename = "node.json"
	raftIdentityFormat   = "skiffdb-raft-node"
	raftIdentityVersion  = uint64(1)
)

type raftIdentity struct {
	Format        string `json:"format"`
	Version       uint64 `json:"version"`
	NodeID        string `json:"node_id"`
	AdvertiseAddr string `json:"advertise_addr"`
}

func resolveRaftIdentity(config *Config) (*raftIdentity, bool, error) {
	if config == nil {
		return nil, false, errors.New("raft config is required")
	}
	path := raftIdentityPath(config)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		legacyIDs, discoverErr := discoverLegacyRaftIDs(config)
		if discoverErr != nil {
			return nil, false, discoverErr
		}
		suppliedID := config.GetRaftId()
		if suppliedID == "" {
			switch len(legacyIDs) {
			case 0:
				return nil, false, errors.New("--raft-id is required for a new Raft node")
			case 1:
				config.setRaftID(legacyIDs[0])
			default:
				return nil, false, fmt.Errorf("--raft-id is required because %d legacy Raft node stores exist under %q", len(legacyIDs), filepath.Join(config.GetDataDir(), "raft"))
			}
		} else if len(legacyIDs) > 0 && !containsString(legacyIDs, suppliedID) {
			return nil, false, fmt.Errorf("--raft-id %q conflicts with existing local Raft store for node %q", suppliedID, strings.Join(legacyIDs, ", "))
		}
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("create raft data directory %q: cannot access persisted identity: %w", filepath.Join(config.GetDataDir(), "raft"), err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var identity raftIdentity
	if err := decoder.Decode(&identity); err != nil {
		return nil, false, fmt.Errorf("decode persisted Raft identity %q: %w", path, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, false, fmt.Errorf("decode persisted Raft identity %q: unexpected trailing JSON value", path)
		}
		return nil, false, fmt.Errorf("decode persisted Raft identity %q: %w", path, err)
	}
	if identity.Format != raftIdentityFormat || identity.Version != raftIdentityVersion {
		return nil, false, fmt.Errorf("unsupported persisted Raft identity format %q version %d", identity.Format, identity.Version)
	}
	if strings.TrimSpace(identity.NodeID) == "" || strings.TrimSpace(identity.AdvertiseAddr) == "" {
		return nil, false, fmt.Errorf("persisted Raft identity %q is incomplete", path)
	}
	if supplied := config.GetRaftId(); supplied != "" && supplied != identity.NodeID {
		return nil, false, fmt.Errorf("--raft-id %q conflicts with persisted node identity %q", supplied, identity.NodeID)
	}
	config.setRaftID(identity.NodeID)
	return &identity, true, nil
}

func discoverLegacyRaftIDs(config *Config) ([]string, error) {
	root := filepath.Join(config.GetDataDir(), "raft")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("create raft data directory %q: cannot scan existing Raft stores: %w", root, err)
	}
	var candidates []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, entry.Name(), "raft.db")); err == nil {
			candidates = append(candidates, entry.Name())
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect legacy Raft store for %q: %w", entry.Name(), err)
		}
	}
	return candidates, nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func persistRaftIdentity(config *Config, advertisedAddress string) error {
	root := filepath.Join(config.GetDataDir(), "raft")
	if err := os.MkdirAll(root, raftDirectoryMode); err != nil {
		return fmt.Errorf("create Raft identity directory %q: %w", root, err)
	}
	if err := os.Chmod(root, raftDirectoryMode); err != nil {
		return fmt.Errorf("set Raft identity directory permissions on %q: %w", root, err)
	}
	path := raftIdentityPath(config)
	identity := raftIdentity{
		Format:        raftIdentityFormat,
		Version:       raftIdentityVersion,
		NodeID:        config.GetRaftId(),
		AdvertiseAddr: strings.TrimSpace(advertisedAddress),
	}
	data, err := json.MarshalIndent(identity, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Raft identity: %w", err)
	}
	data = append(data, '\n')

	temporary, err := os.CreateTemp(root, ".node.json-*")
	if err != nil {
		return fmt.Errorf("create temporary Raft identity in %q: %w", root, err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(raftDatabaseMode); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set temporary Raft identity permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary Raft identity: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary Raft identity: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary Raft identity: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("install persisted Raft identity %q: %w", path, err)
	}
	removeTemporary = false
	directory, err := os.Open(root)
	if err != nil {
		return fmt.Errorf("open Raft identity directory %q for sync: %w", root, err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return fmt.Errorf("sync Raft identity directory %q: %w", root, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close Raft identity directory %q: %w", root, closeErr)
	}
	return nil
}

func raftIdentityPath(config *Config) string {
	return filepath.Join(config.GetDataDir(), "raft", raftIdentityFilename)
}
