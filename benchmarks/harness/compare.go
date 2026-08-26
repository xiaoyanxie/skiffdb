package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

func Compare(baselineDir, candidateDir, outputDir string) (Comparison, error) {
	baselineMetadata, baselineMetrics, err := loadResult(baselineDir)
	if err != nil {
		return Comparison{}, fmt.Errorf("baseline: %w", err)
	}
	candidateMetadata, candidateMetrics, err := loadResult(candidateDir)
	if err != nil {
		return Comparison{}, fmt.Errorf("candidate: %w", err)
	}
	comparison := Comparison{SchemaVersion: SchemaVersion, Baseline: baselineDir, Candidate: candidateDir, Comparable: true}
	if baselineMetadata.OS != candidateMetadata.OS || baselineMetadata.Architecture != candidateMetadata.Architecture || baselineMetadata.CPUCount != candidateMetadata.CPUCount {
		comparison.Comparable = false
		comparison.Warnings = append(comparison.Warnings, "host OS, architecture, or CPU count differs")
	}
	if baselineMetadata.GoVersion != candidateMetadata.GoVersion {
		comparison.Comparable = false
		comparison.Warnings = append(comparison.Warnings, "Go versions differ")
	}
	if baselineMetadata.RandomSeed != candidateMetadata.RandomSeed {
		comparison.Comparable = false
		comparison.Warnings = append(comparison.Warnings, "random seeds differ")
	}
	if baselineMetadata.HotRequestLogging != candidateMetadata.HotRequestLogging {
		comparison.Comparable = false
		comparison.Warnings = append(comparison.Warnings, "hot-path request logging settings differ")
	}
	if filesystemIdentity(baselineMetadata.Filesystem) != filesystemIdentity(candidateMetadata.Filesystem) {
		comparison.Comparable = false
		comparison.Warnings = append(comparison.Warnings, "storage devices or mount points differ")
	}
	if !reflect.DeepEqual(baselineMetadata.Parameters, candidateMetadata.Parameters) {
		comparison.Comparable = false
		comparison.Warnings = append(comparison.Warnings, "benchmark duration, warm-up, concurrency, or pipeline parameters differ")
	}
	if baselineMetadata.Profile != candidateMetadata.Profile {
		comparison.Comparable = false
		comparison.Warnings = append(comparison.Warnings, "benchmark profiles differ")
	}
	if baselineMetadata.GitDirty || candidateMetadata.GitDirty {
		comparison.Warnings = append(comparison.Warnings, "at least one run used a dirty worktree")
	}
	if !baselineMetrics.Completed || !candidateMetrics.Completed {
		comparison.Comparable = false
		comparison.Warnings = append(comparison.Warnings, "at least one run is incomplete")
	}
	baselineValues := comparisonValues(baselineMetrics)
	candidateValues := comparisonValues(candidateMetrics)
	if !sameMetricKeys(baselineValues, candidateValues) {
		comparison.Comparable = false
		comparison.Warnings = append(comparison.Warnings, "result metric sets differ")
	}
	keys := make([]string, 0, len(baselineValues))
	for key := range baselineValues {
		if _, ok := candidateValues[key]; ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		base, candidate := baselineValues[key], candidateValues[key]
		absolute := candidate.value - base.value
		change := MetricComparison{Name: key, BaselineValue: base.value, CandidateValue: candidate.value, AbsoluteChange: absolute, Unit: base.unit}
		if base.value != 0 {
			percent := absolute / base.value * 100
			change.PercentChange = &percent
		}
		comparison.Changes = append(comparison.Changes, change)
	}
	if outputDir == "" {
		outputDir = filepath.Join(candidateDir, "comparison")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return comparison, err
	}
	if err := writeJSON(filepath.Join(outputDir, "comparison.json"), comparison); err != nil {
		return comparison, err
	}
	if err := os.WriteFile(filepath.Join(outputDir, "comparison.md"), []byte(renderComparison(comparison)), 0o644); err != nil {
		return comparison, err
	}
	return comparison, nil
}

func sameMetricKeys(left, right map[string]comparableValue) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, ok := right[key]; !ok {
			return false
		}
	}
	return true
}

func filesystemIdentity(value string) string {
	lines := strings.Split(strings.TrimSpace(value), "\n")
	if len(lines) < 2 {
		return strings.TrimSpace(value)
	}
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 2 {
		return strings.TrimSpace(value)
	}
	return fields[0] + " " + fields[len(fields)-1]
}

type comparableValue struct {
	value float64
	unit  string
}

func comparisonValues(metrics Metrics) map[string]comparableValue {
	values := map[string]comparableValue{}
	for _, result := range metrics.Workloads {
		prefix := fmt.Sprintf("%s/%s/%dB", result.Deployment, result.Workload.Name, result.Workload.ValueSize)
		values[prefix+"/throughput"] = comparableValue{result.Throughput, "ops/s"}
		values[prefix+"/GET-p99"] = comparableValue{result.Operations["GET"].P99US, "us"}
		values[prefix+"/SET-p99"] = comparableValue{result.Operations["SET"].P99US, "us"}
	}
	if value := metrics.Scenarios.SnapshotUnderLoad; value != nil {
		values["scenario/snapshot/duration"] = comparableValue{value.DurationMS, "ms"}
		values["scenario/snapshot/throughput-impact"] = comparableValue{value.ForegroundThroughputChange, "percent"}
		values["scenario/snapshot/max-p99"] = comparableValue{value.MaximumP99US, "us"}
	}
	for _, item := range []struct {
		name  string
		value *RecoveryResult
	}{{"follower-catchup", metrics.Scenarios.FollowerCatchup}, {"leader-failover", metrics.Scenarios.LeaderFailover}, {"snapshot-log-replay", metrics.Scenarios.SnapshotLogReplay}, {"full-cluster-restart", metrics.Scenarios.FullClusterRestart}} {
		if item.value != nil {
			values["scenario/"+item.name] = comparableValue{item.value.DurationMS, "ms"}
		}
	}
	return values
}

func loadResult(dir string) (Metadata, Metrics, error) {
	var metadata Metadata
	var metrics Metrics
	data, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		return metadata, metrics, err
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, metrics, err
	}
	data, err = os.ReadFile(filepath.Join(dir, "metrics.json"))
	if err != nil {
		return metadata, metrics, err
	}
	if err := json.Unmarshal(data, &metrics); err != nil {
		return metadata, metrics, err
	}
	if metadata.SchemaVersion != SchemaVersion || metrics.SchemaVersion != SchemaVersion {
		return metadata, metrics, fmt.Errorf("unsupported schema version metadata=%d metrics=%d", metadata.SchemaVersion, metrics.SchemaVersion)
	}
	return metadata, metrics, nil
}

func renderComparison(comparison Comparison) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# SkiffDB benchmark comparison\n\nBaseline: `%s`  \nCandidate: `%s`  \nComparable: `%t`\n\n", comparison.Baseline, comparison.Candidate, comparison.Comparable)
	for _, warning := range comparison.Warnings {
		fmt.Fprintf(&b, "> Warning: %s\n\n", warning)
	}
	b.WriteString("| Metric | Baseline | Candidate | Absolute change | Change |\n|---|---:|---:|---:|---:|\n")
	for _, change := range comparison.Changes {
		percent := "n/a"
		if change.PercentChange != nil {
			percent = fmt.Sprintf("%+.2f%%", *change.PercentChange)
		}
		fmt.Fprintf(&b, "| %s | %.3f %s | %.3f %s | %+.3f %s | %s |\n", change.Name, change.BaselineValue, change.Unit, change.CandidateValue, change.Unit, change.AbsoluteChange, change.Unit, percent)
	}
	return b.String()
}
