package harness

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFullProfileContainsRequiredMatrix(t *testing.T) {
	profile, err := ResolveProfile("full")
	if err != nil {
		t.Fatal(err)
	}
	if len(profile.Deployments) != 3 {
		t.Fatalf("deployments=%d, want 3", len(profile.Deployments))
	}
	if len(profile.Workloads) != 12 {
		t.Fatalf("workloads=%d, want 12", len(profile.Workloads))
	}
	wantedMixes := map[[2]int]bool{{100, 0}: false, {0, 100}: false, {95, 5}: false, {50, 50}: false}
	wantedSizes := map[int]bool{64: false, 1024: false, 16 * 1024: false}
	for _, workload := range profile.Workloads {
		key := [2]int{workload.GetPercent, workload.SetPercent}
		if _, ok := wantedMixes[key]; !ok {
			t.Fatalf("unexpected mix: %#v", workload)
		}
		wantedMixes[key] = true
		if _, ok := wantedSizes[workload.ValueSize]; !ok {
			t.Fatalf("unexpected size: %d", workload.ValueSize)
		}
		wantedSizes[workload.ValueSize] = true
	}
	for mix, found := range wantedMixes {
		if !found {
			t.Errorf("missing mix %v", mix)
		}
	}
	for size, found := range wantedSizes {
		if !found {
			t.Errorf("missing size %d", size)
		}
	}
	if !profile.Scenarios {
		t.Fatal("full profile omitted recovery scenarios")
	}
}

func TestZeroWarmupRemainsDisabled(t *testing.T) {
	options := Options{Warmup: 0}
	options.setDefaults()
	if options.Warmup != 0 {
		t.Fatalf("warmup=%s, want disabled", options.Warmup)
	}
}

func TestPercentilesUseNearestRank(t *testing.T) {
	values := make([]float64, 1000)
	for i := range values {
		values[i] = float64(i + 1)
	}
	if got := percentile(values, .50); got != 500 {
		t.Errorf("p50=%v", got)
	}
	if got := percentile(values, .95); got != 950 {
		t.Errorf("p95=%v", got)
	}
	if got := percentile(values, .99); got != 990 {
		t.Errorf("p99=%v", got)
	}
	if got := percentile(values, .999); got != 999 {
		t.Errorf("p999=%v", got)
	}
}

func TestRESPClientPipelinesAndParsesBulkStrings(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for i := 0; i < 2; i++ {
			for lineCount := 0; lineCount < 5; lineCount++ {
				_, _ = reader.ReadString('\n')
			}
			_, _ = conn.Write([]byte("$5\r\nvalue\r\n"))
		}
	}()
	client, err := dialRESP(listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	for i := 0; i < 2; i++ {
		if err := writeCommand(client.w, "GET", "key"); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.w.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		response, err := readResponse(client.r)
		if err != nil {
			t.Fatal(err)
		}
		if response != "$5\r\nvalue\r\n" {
			t.Fatalf("response=%q", response)
		}
	}
	client.close()
	<-done
}

func TestAnalyzeSnapshotUsesOverlappingIntervals(t *testing.T) {
	buckets := []IntervalMetrics{{StartMS: 0, EndMS: 100, Operations: 100, P99US: 10}, {StartMS: 100, EndMS: 200, Operations: 50, P99US: 30}, {StartMS: 200, EndMS: 300, Operations: 100, P99US: 12}}
	result := analyzeSnapshot(buckets, hookTiming{start: 120 * time.Millisecond, end: 180 * time.Millisecond}, 100, 180)
	if result.SizeBytes != 80 {
		t.Errorf("size=%d", result.SizeBytes)
	}
	if result.BaselineThroughput != 1000 {
		t.Errorf("baseline=%v", result.BaselineThroughput)
	}
	if result.SnapshotThroughput != 500 {
		t.Errorf("snapshot=%v", result.SnapshotThroughput)
	}
	if result.ForegroundThroughputChange != -50 {
		t.Errorf("change=%v", result.ForegroundThroughputChange)
	}
	if result.MaximumP99US != 30 {
		t.Errorf("p99=%v", result.MaximumP99US)
	}
}

func TestCompareWritesJSONAndMarkdown(t *testing.T) {
	root := t.TempDir()
	baseline := filepath.Join(root, "baseline")
	candidate := filepath.Join(root, "candidate")
	output := filepath.Join(root, "comparison")
	for _, dir := range []string{baseline, candidate} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	metadata := Metadata{SchemaVersion: SchemaVersion, OS: "linux", Architecture: "amd64", CPUCount: 4, Profile: "smoke", Parameters: map[string]any{"concurrency": float64(1)}}
	baseMetrics := Metrics{SchemaVersion: SchemaVersion, Completed: true, Workloads: []WorkloadResult{{Deployment: InMemorySingle, Workload: Workload{Name: "get100", ValueSize: 64}, Throughput: 100, Operations: map[string]OpMetrics{"GET": {P99US: 10}}}}}
	candidateMetrics := baseMetrics
	candidateMetrics.Workloads = []WorkloadResult{{Deployment: InMemorySingle, Workload: Workload{Name: "get100", ValueSize: 64}, Throughput: 110, Operations: map[string]OpMetrics{"GET": {P99US: 9}}}}
	if err := writeJSON(filepath.Join(baseline, "metadata.json"), metadata); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(candidate, "metadata.json"), metadata); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(baseline, "metrics.json"), baseMetrics); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(candidate, "metrics.json"), candidateMetrics); err != nil {
		t.Fatal(err)
	}
	comparison, err := Compare(baseline, candidate, output)
	if err != nil {
		t.Fatal(err)
	}
	if !comparison.Comparable {
		t.Fatalf("warnings=%v", comparison.Warnings)
	}
	if len(comparison.Changes) != 3 {
		t.Fatalf("changes=%d, want 3", len(comparison.Changes))
	}
	data, err := os.ReadFile(filepath.Join(output, "comparison.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "+10.00%") {
		t.Fatalf("markdown did not contain throughput delta:\n%s", data)
	}
}

func TestCompareRejectsDifferentSeeds(t *testing.T) {
	root := t.TempDir()
	baseline := filepath.Join(root, "baseline")
	candidate := filepath.Join(root, "candidate")
	for _, dir := range []string{baseline, candidate} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	metadata := Metadata{SchemaVersion: SchemaVersion, OS: "linux", Architecture: "amd64", CPUCount: 4, GoVersion: "go1.test", Profile: "smoke", RandomSeed: 1, Parameters: map[string]any{}}
	metrics := Metrics{SchemaVersion: SchemaVersion, Completed: true}
	if err := writeJSON(filepath.Join(baseline, "metadata.json"), metadata); err != nil {
		t.Fatal(err)
	}
	metadata.RandomSeed = 2
	if err := writeJSON(filepath.Join(candidate, "metadata.json"), metadata); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(baseline, "metrics.json"), metrics); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(candidate, "metrics.json"), metrics); err != nil {
		t.Fatal(err)
	}
	comparison, err := Compare(baseline, candidate, filepath.Join(root, "comparison"))
	if err != nil {
		t.Fatal(err)
	}
	if comparison.Comparable {
		t.Fatal("comparison with different seeds was marked comparable")
	}
	if !strings.Contains(strings.Join(comparison.Warnings, "\n"), "random seeds differ") {
		t.Fatalf("warnings=%v", comparison.Warnings)
	}
}

func TestSafeCleanupOnlyRemovesHarnessTemporaryDirectory(t *testing.T) {
	root, err := os.MkdirTemp("", "skiffdb-bench-data-")
	if err != nil {
		t.Fatal(err)
	}
	c := cluster{dataRoot: root}
	c.cleanupData()
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("harness temp directory still exists: %v", err)
	}
	userDir := t.TempDir()
	c.dataRoot = userDir
	c.cleanupData()
	if _, err := os.Stat(userDir); err != nil {
		t.Fatalf("user directory was removed: %v", err)
	}
}
