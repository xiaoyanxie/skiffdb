package core

import (
	"flag"
	"strings"
)

type Config struct {
	EnableRaft        bool
	Bootstrap         bool
	raftID            string
	RaftAddr          string
	RaftAdvertiseAddr string
	JoinAddr          string

	// bootstrap string
	// peers     *map[string]string
	AdminAddr string

	ListenAddr  string
	dataDir     string
	maxmemory   string
	LogRequests bool
}

var DBConfig *Config

func InitDBConfig() {
	// raft cluster parameters
	enableRaft := flag.Bool("enable-raft", false, "enable HashiCorp Raft replication")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap a new Raft cluster (new storage only)")
	raftID := flag.String("raft-id", "", "unique Raft node ID (required on first start; reused from disk on restart)")
	raftAddr := flag.String("raft-addr", ":7000", "raft TCP bind address (host:port)")
	raftAdvertiseAddr := flag.String("raft-advertise-addr", "", "Raft address advertised to peers (defaults to --raft-addr)")
	// bootstrap := flag.String("bootstrap-peers", "", "bootstrap peers as 'id1=addr1,id2=addr2'")
	joinAddr := flag.String("join", "", "the cluster node to join")

	// admin
	adminAddr := flag.String("admin-addr", ":50051", "gRPC admin address")

	// database parameters
	listenAddr := flag.String("addr", ":6379", "server listen address")
	dataDir := flag.String("data-dir", "", "the directory used to store data")
	maxmemory := flag.String("maxmemory", "", "the max memory allowed for this instance")
	logRequests := flag.Bool("log-requests", true, "log every request (disable for benchmarks)")

	flag.Parse()

	DBConfig = &Config{
		EnableRaft:        *enableRaft,
		Bootstrap:         *bootstrap,
		raftID:            strings.TrimSpace(*raftID),
		RaftAddr:          strings.TrimSpace(*raftAddr),
		RaftAdvertiseAddr: strings.TrimSpace(*raftAdvertiseAddr),
		JoinAddr:          strings.TrimSpace(*joinAddr),

		AdminAddr: strings.TrimSpace(*adminAddr),

		ListenAddr:  strings.TrimSpace(*listenAddr),
		dataDir:     strings.TrimSpace(*dataDir),
		maxmemory:   strings.TrimSpace(*maxmemory),
		LogRequests: *logRequests,
	}
}

func (config *Config) getDefaultDataDir() string {
	return "/var/lib/skiffdb"
}

func (config *Config) GetDataDir() string {
	if config.dataDir == "" {
		return config.getDefaultDataDir()
	}
	return config.dataDir
}

// func (config *Config) parsePeers() *map[string]string {
// 	res := map[string]string{}
// 	if strings.TrimSpace(config.bootstrap) == "" {
// 		return &res
// 	}
// 	parts := strings.Split(config.bootstrap, ",")
// 	for _, p := range parts {
// 		p = strings.TrimSpace(p)
// 		kv := strings.SplitN(p, "=", 2)
// 		if len(kv) != 2 {
// 			continue
// 		}
// 		id := strings.TrimSpace(kv[0])
// 		addr := strings.TrimSpace(kv[1])
// 		if id != "" && addr != "" {
// 			res[id] = addr
// 		}
// 	}
// 	return &res
// }

// func (config *Config) GetPeers() map[string]string {
// 	if config.peers == nil {
// 		config.peers = config.parsePeers()
// 	}
// 	return *config.peers
// }

func (config *Config) GetRaftId() string {
	return strings.TrimSpace(config.raftID)
}

func (config *Config) setRaftID(nodeID string) {
	config.raftID = strings.TrimSpace(nodeID)
}

func (config *Config) GetRaftAdvertiseAddr() string {
	if advertised := strings.TrimSpace(config.RaftAdvertiseAddr); advertised != "" {
		return advertised
	}
	return strings.TrimSpace(config.RaftAddr)
}
