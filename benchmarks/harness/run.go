package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func Run(ctx context.Context, options Options) (*Bundle, error) {
	options.setDefaults()
	profile, err := ResolveProfile(options.Profile)
	if err != nil {
		return nil, err
	}
	metadata, err := collectMetadata(options)
	if err != nil {
		return nil, err
	}
	dirName := metadata.StartTime.UTC().Format("20060102T150405.000000000Z") + "-" + shortSHA(metadata.GitCommit)
	resultDir := filepath.Join(options.ResultsRoot, dirName)
	logsDir := filepath.Join(resultDir, "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return nil, err
	}
	bundle := &Bundle{Dir: resultDir, Metadata: metadata, Metrics: Metrics{
		SchemaVersion: SchemaVersion, HarnessVersion: HarnessVersion,
		MissingMetrics: []string{"Raft commit/apply latency: server instrumentation is not available", "Raft follower lag: server instrumentation is not available"},
	}}
	if err := persistBundle(bundle); err != nil {
		return bundle, err
	}
	if err := buildServer(ctx, options.BuildCommand, filepath.Join(logsDir, "build.log")); err != nil {
		bundle.Metrics.Error = err.Error()
		_ = persistBundle(bundle)
		return bundle, err
	}
	if _, err := os.Stat(options.ServerBinary); err != nil {
		runErr := fmt.Errorf("server binary %q after build: %w", options.ServerBinary, err)
		bundle.Metrics.Error = runErr.Error()
		_ = persistBundle(bundle)
		return bundle, runErr
	}

	for _, deployment := range profile.Deployments {
		c, err := startCluster(ctx, deployment, options.ServerBinary, logsDir, options.KeepData)
		if err != nil {
			return failBundle(bundle, err)
		}
		bundle.Metadata.Configurations = append(bundle.Metadata.Configurations, describeCluster(c, "baseline-matrix"))
		if err := persistBundle(bundle); err != nil {
			_ = c.stopAll()
			c.cleanupData()
			return bundle, err
		}
		leader, err := findLeader(c.nodes)
		if deployment == InMemorySingle {
			leader = 0
			err = nil
		}
		if err != nil {
			_ = c.stopAll()
			c.cleanupData()
			return failBundle(bundle, err)
		}
		for _, workload := range profile.Workloads {
			result, _, runErr := executeWorkload(ctx, c.nodes[leader].respAddr, deployment, workload, options, 0, nil)
			bundle.Metrics.Workloads = append(bundle.Metrics.Workloads, result)
			if err := persistBundle(bundle); err != nil {
				_ = c.stopAll()
				c.cleanupData()
				return bundle, err
			}
			if runErr != nil {
				_ = c.stopAll()
				c.cleanupData()
				return failBundle(bundle, fmt.Errorf("%s/%s: %w", deployment, workload.Name, runErr))
			}
		}
		_ = c.stopAll()
		bundle.Metrics.Processes = append(bundle.Metrics.Processes, c.metrics...)
		c.cleanupData()
		if err := persistBundle(bundle); err != nil {
			return bundle, err
		}
	}

	if profile.Scenarios {
		if err := runScenarios(ctx, bundle, options, logsDir); err != nil {
			return failBundle(bundle, err)
		}
	}
	bundle.Metrics.Completed = true
	bundle.Metrics.Error = ""
	if err := persistBundle(bundle); err != nil {
		return bundle, err
	}
	return bundle, nil
}

func runScenarios(ctx context.Context, bundle *Bundle, options Options, logsDir string) error {
	c, err := startCluster(ctx, DurableThree, options.ServerBinary, logsDir, options.KeepData)
	if err != nil {
		return err
	}
	defer func() { _ = c.stopAll(); c.cleanupData() }()
	bundle.Metadata.Configurations = append(bundle.Metadata.Configurations, describeCluster(c, "snapshot-and-recovery-scenarios"))
	if err := persistBundle(bundle); err != nil {
		return err
	}
	leader, err := findLeader(c.nodes)
	if err != nil {
		return err
	}

	scenarioOpts := options
	if scenarioOpts.Duration < 1200*time.Millisecond {
		scenarioOpts.Duration = 1200 * time.Millisecond
	}
	workload := Workload{Name: "snapshot-under-load-get95-set5", GetPercent: 95, SetPercent: 5, ValueSize: 1024}
	sizeBefore := c.nodes[leader].snapshotSize()
	result, timing, runErr := executeWorkload(ctx, c.nodes[leader].respAddr, DurableThree, workload, scenarioOpts, scenarioOpts.Duration/3, func() error {
		response, err := commandAt(c.nodes[leader].respAddr, "SAVE")
		if err != nil {
			return err
		}
		if !strings.HasPrefix(response, "+OK") {
			return fmt.Errorf("SAVE returned %q", strings.TrimSpace(response))
		}
		return nil
	})
	bundle.Metrics.Workloads = append(bundle.Metrics.Workloads, result)
	snapshot := analyzeSnapshot(result.Buckets, timing, sizeBefore, c.nodes[leader].snapshotSize())
	if runErr != nil && snapshot.Error == "" {
		snapshot.Error = runErr.Error()
		snapshot.Success = false
	}
	bundle.Metrics.Scenarios.SnapshotUnderLoad = &snapshot
	if err := persistBundle(bundle); err != nil {
		return err
	}
	if runErr != nil {
		return fmt.Errorf("snapshot workload: %w", runErr)
	}
	if timing.err != nil {
		return fmt.Errorf("snapshot scenario: %w", timing.err)
	}

	leader, err = findLeader(c.nodes)
	if err != nil {
		return err
	}
	follower := 0
	if follower == leader {
		follower = 1
	}
	if err := c.stopNode(follower); err != nil {
		return err
	}
	marker := strconv.FormatInt(time.Now().UnixNano(), 10)
	if response, err := commandAt(c.nodes[leader].respAddr, "SET", "bench:catchup", marker); err != nil || !strings.HasPrefix(response, "+OK") {
		return fmt.Errorf("write catch-up marker: response=%q err=%v", response, err)
	}
	started := time.Now()
	if err := c.restartNode(ctx, follower); err != nil {
		return err
	}
	catchupErr := waitForValue(c.nodes[follower].respAddr, "bench:catchup", marker, 15*time.Second)
	bundle.Metrics.Scenarios.FollowerCatchup = recoveryResult(started, catchupErr)
	if err := persistBundle(bundle); err != nil {
		return err
	}
	if catchupErr != nil {
		return fmt.Errorf("follower catch-up: %w", catchupErr)
	}

	leader, err = findLeader(c.nodes)
	if err != nil {
		return err
	}
	started = time.Now()
	if err := c.stopNode(leader); err != nil {
		return err
	}
	failoverErr := waitForWritable(c.nodes, 15*time.Second)
	bundle.Metrics.Scenarios.LeaderFailover = recoveryResult(started, failoverErr)
	if err := persistBundle(bundle); err != nil {
		return err
	}
	if failoverErr != nil {
		return fmt.Errorf("leader failover: %w", failoverErr)
	}
	if err := c.restartNode(ctx, leader); err != nil {
		return fmt.Errorf("restart former leader: %w", err)
	}
	if err := waitForWritable(c.nodes, 15*time.Second); err != nil {
		return err
	}

	leader, err = findLeader(c.nodes)
	if err != nil {
		return err
	}
	if response, err := commandAt(c.nodes[leader].respAddr, "SET", "bench:snapshot-state", "captured"); err != nil || !strings.HasPrefix(response, "+OK") {
		return fmt.Errorf("write snapshot state: %q %v", response, err)
	}
	if response, err := commandAt(c.nodes[leader].respAddr, "SAVE"); err != nil || !strings.HasPrefix(response, "+OK") {
		return fmt.Errorf("create recovery snapshot: %q %v", response, err)
	}
	if response, err := commandAt(c.nodes[leader].respAddr, "SET", "bench:log-tail", "replayed"); err != nil || !strings.HasPrefix(response, "+OK") {
		return fmt.Errorf("write log tail: %q %v", response, err)
	}
	_ = c.stopAll()
	started = time.Now()
	for index := range c.nodes {
		if err := c.restartNode(ctx, index); err != nil {
			return fmt.Errorf("restart cluster node %d: %w", index, err)
		}
	}
	restartErr := waitForWritable(c.nodes, 20*time.Second)
	if restartErr == nil {
		for _, n := range c.nodes {
			if err := waitForValue(n.respAddr, "bench:snapshot-state", "captured", 10*time.Second); err != nil {
				restartErr = err
				break
			}
			if err := waitForValue(n.respAddr, "bench:log-tail", "replayed", 10*time.Second); err != nil {
				restartErr = err
				break
			}
		}
	}
	recovery := recoveryResult(started, restartErr)
	bundle.Metrics.Scenarios.SnapshotLogReplay = recovery
	fullRestart := *recovery
	bundle.Metrics.Scenarios.FullClusterRestart = &fullRestart
	_ = c.stopAll()
	bundle.Metrics.Processes = append(bundle.Metrics.Processes, c.metrics...)
	c.cleanupData()
	if err := persistBundle(bundle); err != nil {
		return err
	}
	if restartErr != nil {
		return fmt.Errorf("full cluster recovery: %w", restartErr)
	}
	return nil
}

func analyzeSnapshot(buckets []IntervalMetrics, timing hookTiming, sizeBefore, sizeAfter uint64) SnapshotResult {
	result := SnapshotResult{DurationMS: millis(timing.end - timing.start), Success: timing.err == nil}
	if sizeAfter > sizeBefore {
		result.SizeBytes = sizeAfter - sizeBefore
	}
	if timing.err != nil {
		result.Error = timing.err.Error()
	}
	var baselineOps, snapshotOps uint64
	var baselineSeconds, snapshotSeconds float64
	for _, bucket := range buckets {
		bucketStart, bucketEnd := time.Duration(bucket.StartMS*1e6), time.Duration(bucket.EndMS*1e6)
		if bucketEnd <= timing.start {
			baselineOps += bucket.Operations
			baselineSeconds += (bucket.EndMS - bucket.StartMS) / 1000
		}
		if bucketEnd > timing.start && bucketStart < timing.end {
			snapshotOps += bucket.Operations
			snapshotSeconds += (bucket.EndMS - bucket.StartMS) / 1000
			if bucket.P99US > result.MaximumP99US {
				result.MaximumP99US = bucket.P99US
			}
		}
	}
	if baselineSeconds > 0 {
		result.BaselineThroughput = float64(baselineOps) / baselineSeconds
	}
	if snapshotSeconds > 0 {
		result.SnapshotThroughput = float64(snapshotOps) / snapshotSeconds
	}
	if result.BaselineThroughput > 0 {
		result.ForegroundThroughputChange = (result.SnapshotThroughput/result.BaselineThroughput - 1) * 100
	}
	return result
}

func recoveryResult(start time.Time, err error) *RecoveryResult {
	result := &RecoveryResult{DurationMS: millis(time.Since(start)), Success: err == nil}
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func waitForValue(addr, key, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := commandAt(addr, "GET", key)
		if err == nil && strings.Contains(response, "\r\n"+want+"\r\n") {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for key %q on %s", key, addr)
}

func buildServer(ctx context.Context, command, logPath string) error {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build command %q failed (see %s): %w", command, logPath, err)
	}
	return nil
}

func failBundle(bundle *Bundle, err error) (*Bundle, error) {
	bundle.Metrics.Error = err.Error()
	_ = persistBundle(bundle)
	return bundle, err
}

func persistBundle(bundle *Bundle) error {
	if err := writeJSON(filepath.Join(bundle.Dir, "metadata.json"), bundle.Metadata); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(bundle.Dir, "metrics.json"), bundle.Metrics); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(bundle.Dir, "summary.md"), []byte(renderSummary(bundle)), 0o644)
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func collectMetadata(options Options) (Metadata, error) {
	shaOutput, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return Metadata{}, fmt.Errorf("git commit: %w", err)
	}
	statusOutput, err := exec.Command("git", "status", "--porcelain", "--untracked-files=normal").Output()
	if err != nil {
		return Metadata{}, fmt.Errorf("git status: %w", err)
	}
	metadata := Metadata{
		SchemaVersion: SchemaVersion, HarnessVersion: HarnessVersion, StartTime: time.Now().UTC(), GitCommit: strings.TrimSpace(string(shaOutput)), GitDirty: len(statusOutput) > 0,
		BuildCommand: options.BuildCommand, GoVersion: runtime.Version(), OS: runtime.GOOS, Architecture: runtime.GOARCH, CPUCount: runtime.NumCPU(),
		StoragePath: options.ResultsRoot, RandomSeed: options.Seed, Profile: options.Profile, HotRequestLogging: false,
		Parameters: map[string]any{
			"duration_ms": millis(options.Duration), "warmup_ms": millis(options.Warmup),
			"concurrency": options.Concurrency, "pipeline_depth": options.PipelineDepth,
			"key_distribution": "uniform", "key_count": 256,
		},
	}
	metadata.AvailableMemory = availableMemory()
	if metadata.AvailableMemory == 0 {
		metadata.Missing = append(metadata.Missing, "available memory")
	}
	if output, err := exec.Command("df", "-P", existingPath(options.ResultsRoot)).Output(); err == nil {
		metadata.Filesystem = strings.TrimSpace(string(output))
	} else {
		metadata.Missing = append(metadata.Missing, "filesystem information")
	}
	return metadata, nil
}

func existingPath(path string) string {
	if strings.TrimSpace(path) == "" {
		return "."
	}
	current := filepath.Clean(path)
	for {
		if _, err := os.Stat(current); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "."
		}
		current = parent
	}
}

func availableMemory() uint64 {
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "MemAvailable:" {
				value, _ := strconv.ParseUint(fields[1], 10, 64)
				return value * 1024
			}
		}
	}
	if output, err := exec.Command("vm_stat").Output(); err == nil {
		var pageSize uint64 = 4096
		var pages uint64
		for _, line := range strings.Split(string(output), "\n") {
			if strings.Contains(line, "page size of") {
				fields := strings.Fields(line)
				for index, field := range fields {
					if field == "of" && index+1 < len(fields) {
						pageSize, _ = strconv.ParseUint(fields[index+1], 10, 64)
					}
				}
			}
			if strings.HasPrefix(line, "Pages free:") || strings.HasPrefix(line, "Pages inactive:") || strings.HasPrefix(line, "Pages speculative:") {
				fields := strings.Fields(line)
				if len(fields) > 0 {
					value, _ := strconv.ParseUint(strings.TrimSuffix(fields[len(fields)-1], "."), 10, 64)
					pages += value
				}
			}
		}
		if pages > 0 {
			return pages * pageSize
		}
	}
	return 0
}

func describeCluster(c *cluster, purpose string) DeploymentConfig {
	configuration := DeploymentConfig{Deployment: c.deployment, Purpose: purpose}
	for _, n := range c.nodes {
		nodeConfiguration := NodeConfiguration{
			NodeID: n.id, RESPAddress: n.respAddr, AdminAddress: n.adminAddr,
			RaftEnabled: c.deployment != InMemorySingle, DataDirectory: n.dataDir,
			Bootstrap: n.bootstrap, JoinAddress: n.joinAddr, RequestLogging: false,
		}
		if nodeConfiguration.RaftEnabled {
			nodeConfiguration.RaftAddress = n.raftAddr
			nodeConfiguration.RaftAdvertiseAddr = n.raftAddr
		}
		configuration.Nodes = append(configuration.Nodes, nodeConfiguration)
	}
	return configuration
}

func shortSHA(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	if sha == "" {
		return "unknown"
	}
	return sha
}

func renderSummary(bundle *Bundle) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# SkiffDB benchmark summary\n\nCommit: `%s` (dirty: `%t`)  \nProfile: `%s`  \nCompleted: `%t`\n\n", bundle.Metadata.GitCommit, bundle.Metadata.GitDirty, bundle.Metadata.Profile, bundle.Metrics.Completed)
	if bundle.Metrics.Error != "" {
		fmt.Fprintf(&b, "> Run error: %s\n\n", bundle.Metrics.Error)
	}
	b.WriteString("| Deployment | Workload | Value | Throughput ops/s | GET p99 µs | SET p99 µs | Errors |\n|---|---|---:|---:|---:|---:|---:|\n")
	for _, result := range bundle.Metrics.Workloads {
		fmt.Fprintf(&b, "| %s | %s | %d B | %.2f | %.2f | %.2f | %d |\n", result.Deployment, result.Workload.Name, result.Workload.ValueSize, result.Throughput, result.Operations["GET"].P99US, result.Operations["SET"].P99US, result.ErrorCount)
	}
	b.WriteString("\n## Recovery and snapshot scenarios\n\n")
	if value := bundle.Metrics.Scenarios.SnapshotUnderLoad; value != nil {
		fmt.Fprintf(&b, "- Snapshot under load: %.2f ms, %d bytes, throughput change %.2f%%, max p99 %.2f µs (success: %t)\n", value.DurationMS, value.SizeBytes, value.ForegroundThroughputChange, value.MaximumP99US, value.Success)
	}
	for _, item := range []struct {
		name  string
		value *RecoveryResult
	}{{"Follower catch-up", bundle.Metrics.Scenarios.FollowerCatchup}, {"Leader failover", bundle.Metrics.Scenarios.LeaderFailover}, {"Snapshot + log replay", bundle.Metrics.Scenarios.SnapshotLogReplay}, {"Full-cluster restart", bundle.Metrics.Scenarios.FullClusterRestart}} {
		if item.value != nil {
			fmt.Fprintf(&b, "- %s: %.2f ms (success: %t)\n", item.name, item.value.DurationMS, item.value.Success)
		}
	}
	b.WriteString("\n## Missing internal metrics\n\n")
	for _, missing := range bundle.Metrics.MissingMetrics {
		fmt.Fprintf(&b, "- %s\n", missing)
	}
	b.WriteString("\nDo not compare deployment modes as equivalent: in-memory acknowledgements and durable Raft acknowledgements provide different durability guarantees. Compare historical runs only when workload parameters and host metadata match.\n")
	return b.String()
}
