package harness

import "time"

const (
	SchemaVersion  = 1
	HarnessVersion = "1.0.0"
)

type Options struct {
	Profile       string
	ResultsRoot   string
	ServerBinary  string
	BuildCommand  string
	Duration      time.Duration
	Warmup        time.Duration
	Concurrency   int
	PipelineDepth int
	KeyCount      int
	Seed          int64
	KeepData      bool
	Orchestration string
}

type Profile struct {
	Name        string
	Deployments []Deployment
	Workloads   []Workload
	Scenarios   bool
}

type Deployment string

const (
	InMemorySingle Deployment = "in-memory-single"
	DurableSingle  Deployment = "durable-single-voter"
	DurableThree   Deployment = "durable-three-voter"
)

type Workload struct {
	Name       string `json:"name"`
	GetPercent int    `json:"get_percent"`
	SetPercent int    `json:"set_percent"`
	ValueSize  int    `json:"value_size_bytes"`
}

type Metadata struct {
	SchemaVersion     int                `json:"schema_version"`
	HarnessVersion    string             `json:"harness_version"`
	StartTime         time.Time          `json:"start_time"`
	GitCommit         string             `json:"git_commit"`
	GitDirty          bool               `json:"git_dirty"`
	BuildCommand      string             `json:"build_command"`
	GoVersion         string             `json:"go_version"`
	OS                string             `json:"os"`
	Architecture      string             `json:"architecture"`
	CPUCount          int                `json:"cpu_count"`
	AvailableMemory   uint64             `json:"available_memory_bytes,omitempty"`
	StoragePath       string             `json:"storage_path"`
	Filesystem        string             `json:"filesystem,omitempty"`
	RandomSeed        int64              `json:"random_seed"`
	Profile           string             `json:"profile"`
	Orchestration     string             `json:"orchestration"`
	HotRequestLogging bool               `json:"hot_path_request_logging"`
	Parameters        map[string]any     `json:"parameters"`
	Configurations    []DeploymentConfig `json:"skiffdb_configurations"`
	Missing           []string           `json:"missing_metadata,omitempty"`
}

type DeploymentConfig struct {
	Deployment Deployment          `json:"deployment"`
	Purpose    string              `json:"purpose"`
	Nodes      []NodeConfiguration `json:"nodes"`
}

type NodeConfiguration struct {
	NodeID            string `json:"node_id"`
	RESPAddress       string `json:"resp_address"`
	AdminAddress      string `json:"admin_address"`
	RaftEnabled       bool   `json:"raft_enabled"`
	RaftAddress       string `json:"raft_address,omitempty"`
	RaftAdvertiseAddr string `json:"raft_advertise_address,omitempty"`
	DataDirectory     string `json:"data_directory"`
	Bootstrap         bool   `json:"bootstrap"`
	JoinAddress       string `json:"join_address,omitempty"`
	RequestLogging    bool   `json:"request_logging"`
}

type Metrics struct {
	SchemaVersion  int              `json:"schema_version"`
	HarnessVersion string           `json:"harness_version"`
	Completed      bool             `json:"completed"`
	Error          string           `json:"error,omitempty"`
	Workloads      []WorkloadResult `json:"workloads"`
	Scenarios      ScenarioResults  `json:"scenarios"`
	Processes      []ProcessMetrics `json:"processes"`
	MissingMetrics []string         `json:"missing_metrics,omitempty"`
}

type WorkloadResult struct {
	Deployment   Deployment           `json:"deployment"`
	Workload     Workload             `json:"workload"`
	DurationMS   float64              `json:"duration_ms"`
	WarmupMS     float64              `json:"warmup_ms"`
	Concurrency  int                  `json:"concurrency"`
	Pipeline     int                  `json:"pipeline_depth"`
	Seed         int64                `json:"seed"`
	Throughput   float64              `json:"throughput_ops_per_second"`
	SuccessCount uint64               `json:"successful_operations"`
	ErrorCount   uint64               `json:"error_operations"`
	Operations   map[string]OpMetrics `json:"operations"`
	Buckets      []IntervalMetrics    `json:"intervals,omitempty"`
}

type OpMetrics struct {
	Count  uint64  `json:"count"`
	P50US  float64 `json:"p50_us"`
	P95US  float64 `json:"p95_us"`
	P99US  float64 `json:"p99_us"`
	P999US float64 `json:"p999_us"`
}

type IntervalMetrics struct {
	StartMS    float64 `json:"start_ms"`
	EndMS      float64 `json:"end_ms"`
	Operations uint64  `json:"operations"`
	Throughput float64 `json:"throughput_ops_per_second"`
	P99US      float64 `json:"p99_us"`
}

type ProcessMetrics struct {
	Deployment     Deployment `json:"deployment"`
	NodeID         string     `json:"node_id"`
	PID            int        `json:"pid"`
	CPUTimeMS      float64    `json:"cpu_time_ms,omitempty"`
	PeakRSSBytes   uint64     `json:"peak_rss_bytes,omitempty"`
	DiskReadBytes  uint64     `json:"disk_read_bytes,omitempty"`
	DiskWriteBytes uint64     `json:"disk_write_bytes,omitempty"`
	Missing        []string   `json:"missing,omitempty"`
}

type ScenarioResults struct {
	SnapshotUnderLoad  *SnapshotResult `json:"snapshot_under_load,omitempty"`
	FollowerCatchup    *RecoveryResult `json:"follower_catchup,omitempty"`
	LeaderFailover     *RecoveryResult `json:"leader_failover,omitempty"`
	SnapshotLogReplay  *RecoveryResult `json:"snapshot_plus_log_replay,omitempty"`
	FullClusterRestart *RecoveryResult `json:"full_cluster_restart,omitempty"`
}

type SnapshotResult struct {
	DurationMS                 float64 `json:"duration_ms"`
	SizeBytes                  uint64  `json:"size_bytes"`
	BaselineThroughput         float64 `json:"baseline_throughput_ops_per_second"`
	SnapshotThroughput         float64 `json:"snapshot_throughput_ops_per_second"`
	ForegroundThroughputChange float64 `json:"foreground_throughput_change_percent"`
	MaximumP99US               float64 `json:"maximum_p99_us"`
	Success                    bool    `json:"success"`
	Error                      string  `json:"error,omitempty"`
}

type RecoveryResult struct {
	DurationMS float64 `json:"duration_ms"`
	Success    bool    `json:"success"`
	Error      string  `json:"error,omitempty"`
}

type Bundle struct {
	Dir      string
	Metadata Metadata
	Metrics  Metrics
}

type Comparison struct {
	SchemaVersion int                `json:"schema_version"`
	Baseline      string             `json:"baseline"`
	Candidate     string             `json:"candidate"`
	Comparable    bool               `json:"comparable"`
	Warnings      []string           `json:"warnings,omitempty"`
	Changes       []MetricComparison `json:"changes"`
}

type MetricComparison struct {
	Name           string   `json:"name"`
	BaselineValue  float64  `json:"baseline_value"`
	CandidateValue float64  `json:"candidate_value"`
	AbsoluteChange float64  `json:"absolute_change"`
	PercentChange  *float64 `json:"percent_change,omitempty"`
	Unit           string   `json:"unit"`
}
