package harness

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// RunRemote executes benchmark workloads against an already-running
// deployment. It never starts, stops, or otherwise owns the target processes.
// Recovery and snapshot scenarios remain the responsibility of an external
// orchestrator because endpoints alone are not authority to terminate nodes or
// inspect their storage.
func RunRemote(ctx context.Context, options Options, deployment Deployment, targets []string) (*Bundle, error) {
	options.Orchestration = "remote-target"
	options.setDefaults()
	profile, err := ResolveProfile(options.Profile)
	if err != nil {
		return nil, err
	}
	targets, err = normalizeTargets(targets)
	if err != nil {
		return nil, err
	}
	if deployment != InMemorySingle && deployment != DurableSingle && deployment != DurableThree {
		return nil, fmt.Errorf("unsupported remote deployment %q", deployment)
	}
	metadata, err := collectMetadata(options)
	if err != nil {
		return nil, err
	}
	metadata.BuildCommand = "not run (remote target)"
	metadata.Configurations = []DeploymentConfig{{
		Deployment: deployment,
		Purpose:    "remote-target-workloads",
		Nodes:      remoteNodeConfigurations(targets, deployment),
	}}
	metadata.Parameters["remote_target_count"] = len(targets)
	metadata.Parameters["recovery_scenarios"] = false

	dirName := metadata.StartTime.UTC().Format("20060102T150405.000000000Z") + "-" + shortSHA(metadata.GitCommit)
	resultDir := filepath.Join(options.ResultsRoot, dirName)
	if err := os.MkdirAll(filepath.Join(resultDir, "logs"), 0o755); err != nil {
		return nil, err
	}
	bundle := &Bundle{Dir: resultDir, Metadata: metadata, Metrics: Metrics{
		SchemaVersion:  SchemaVersion,
		HarnessVersion: HarnessVersion,
		MissingMetrics: []string{
			"remote server process CPU, RSS, and disk counters are not collected",
			"Raft commit/apply latency: server instrumentation is not available",
			"Raft follower lag: server instrumentation is not available",
			"remote snapshot and recovery scenarios require an external orchestrator",
		},
	}}
	if err := persistBundle(bundle); err != nil {
		return bundle, err
	}

	for _, workload := range profile.Workloads {
		target, _, findErr := FindWritableTarget(targets, deployment)
		if findErr != nil {
			return failBundle(bundle, findErr)
		}
		result, _, runErr := executeWorkload(ctx, target, deployment, workload, options, 0, nil)
		bundle.Metrics.Workloads = append(bundle.Metrics.Workloads, result)
		if err := persistBundle(bundle); err != nil {
			return bundle, err
		}
		if runErr != nil {
			return failBundle(bundle, fmt.Errorf("%s/%s: %w", deployment, workload.Name, runErr))
		}
	}
	bundle.Metrics.Completed = true
	if err := persistBundle(bundle); err != nil {
		return bundle, err
	}
	return bundle, nil
}

func normalizeTargets(targets []string) ([]string, error) {
	result := make([]string, 0, len(targets))
	seen := make(map[string]bool, len(targets))
	for _, target := range targets {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(target); err != nil {
			return nil, fmt.Errorf("invalid RESP target %q: %w", target, err)
		}
		if !seen[target] {
			seen[target] = true
			result = append(result, target)
		}
	}
	if len(result) == 0 {
		return nil, errors.New("at least one RESP target is required")
	}
	return result, nil
}

func remoteNodeConfigurations(targets []string, deployment Deployment) []NodeConfiguration {
	result := make([]NodeConfiguration, 0, len(targets))
	for index := range targets {
		result = append(result, NodeConfiguration{
			NodeID:         fmt.Sprintf("remote-node-%d", index+1),
			RESPAddress:    "<redacted>",
			RaftEnabled:    deployment != InMemorySingle,
			RequestLogging: false,
		})
	}
	return result
}

// FindWritableTarget returns the endpoint and input index of the node that can
// acknowledge a SET. In Raft deployments followers reject this probe and only
// the leader succeeds.
func FindWritableTarget(targets []string, deployment Deployment) (string, int, error) {
	targets, err := normalizeTargets(targets)
	if err != nil {
		return "", -1, err
	}
	for index, target := range targets {
		if deployment == InMemorySingle {
			if err := waitForRESP(target, 500*time.Millisecond); err == nil {
				return target, index, nil
			}
			continue
		}
		response, commandErr := commandAt(target, "SET", "bench:leader-probe", strconv.FormatInt(time.Now().UnixNano(), 10))
		if commandErr == nil && strings.HasPrefix(response, "+OK") {
			return target, index, nil
		}
	}
	return "", -1, errors.New("no writable target found")
}

// SetOnWritableTarget writes a marker through the current leader and returns
// the endpoint and its input index.
func SetOnWritableTarget(targets []string, deployment Deployment, key, value string) (string, int, error) {
	target, index, err := FindWritableTarget(targets, deployment)
	if err != nil {
		return "", -1, err
	}
	response, err := commandAt(target, "SET", key, value)
	if err != nil {
		return "", -1, err
	}
	if !strings.HasPrefix(response, "+OK") {
		return "", -1, fmt.Errorf("SET returned %q", strings.TrimSpace(response))
	}
	return target, index, nil
}

// WaitForRemoteValue waits until a target has applied a replicated marker.
func WaitForRemoteValue(target, key, value string, timeout time.Duration) error {
	return waitForValue(target, key, value, timeout)
}
