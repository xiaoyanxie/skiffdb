package harness

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type cluster struct {
	deployment Deployment
	binary     string
	dataRoot   string
	logsDir    string
	nodes      []*node
	metrics    []ProcessMetrics
	keepData   bool
}

type node struct {
	id         string
	respAddr   string
	adminAddr  string
	raftAddr   string
	dataDir    string
	bootstrap  bool
	joinAddr   string
	binary     string
	logPath    string
	cmd        *exec.Cmd
	done       chan struct{}
	waitErr    error
	state      *os.ProcessState
	peakRSS    uint64
	diskStart  diskCounters
	diskEnd    diskCounters
	mu         sync.Mutex
	stopSample chan struct{}
}

type diskCounters struct {
	readBytes, writeBytes uint64
	ok                    bool
}

func startCluster(ctx context.Context, deployment Deployment, binary, logsDir string, keepData bool) (*cluster, error) {
	dataRoot, err := os.MkdirTemp("", "skiffdb-bench-data-")
	if err != nil {
		return nil, err
	}
	c := &cluster{deployment: deployment, binary: binary, dataRoot: dataRoot, logsDir: logsDir, keepData: keepData}
	cleanupOnError := func(runErr error) (*cluster, error) {
		_ = c.stopAll()
		c.cleanupData()
		return nil, runErr
	}
	nodeCount := 1
	if deployment == DurableThree {
		nodeCount = 3
	}
	ports, err := reservePorts(nodeCount * 3)
	if err != nil {
		return cleanupOnError(err)
	}
	for i := 0; i < nodeCount; i++ {
		n := &node{
			id: fmt.Sprintf("bench-node-%d", i+1), respAddr: ports[i*3], adminAddr: ports[i*3+1], raftAddr: ports[i*3+2],
			dataDir: filepath.Join(dataRoot, fmt.Sprintf("node-%d", i+1)), binary: binary,
			logPath: filepath.Join(logsDir, fmt.Sprintf("%s-node-%d.log", deployment, i+1)),
		}
		if deployment != InMemorySingle {
			n.bootstrap = i == 0
			if i > 0 {
				n.joinAddr = c.nodes[0].adminAddr
			}
		}
		c.nodes = append(c.nodes, n)
		if err := n.start(ctx, deployment); err != nil {
			return cleanupOnError(fmt.Errorf("start %s: %w", n.id, err))
		}
		if err := waitForRESP(n.respAddr, 15*time.Second); err != nil {
			return cleanupOnError(fmt.Errorf("wait for %s: %w", n.id, err))
		}
		if i == 0 && deployment != InMemorySingle {
			if err := waitForWritable([]*node{n}, 10*time.Second); err != nil {
				return cleanupOnError(err)
			}
		}
	}
	if err := waitForWritable(c.nodes, 15*time.Second); err != nil {
		return cleanupOnError(err)
	}
	return c, nil
}

func (n *node) start(ctx context.Context, deployment Deployment) error {
	if n.cmd != nil {
		return errors.New("node is already running")
	}
	if err := os.MkdirAll(n.dataDir, 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(n.logPath), 0o755); err != nil {
		return err
	}
	logFile, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	args := []string{"--addr=" + n.respAddr, "--admin-addr=" + n.adminAddr, "--data-dir=" + n.dataDir, "--log-requests=false"}
	if deployment != InMemorySingle {
		args = append(args, "--enable-raft", "--raft-id="+n.id, "--raft-addr="+n.raftAddr, "--raft-advertise-addr="+n.raftAddr)
		if n.bootstrap {
			args = append(args, "--bootstrap")
		}
		if n.joinAddr != "" {
			args = append(args, "--join="+n.joinAddr)
		}
	}
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	n.cmd = cmd
	n.peakRSS = 0
	n.state = nil
	n.waitErr = nil
	n.diskEnd = diskCounters{}
	n.done = make(chan struct{})
	n.stopSample = make(chan struct{})
	n.diskStart = readDiskCounters(cmd.Process.Pid)
	go n.sampleRSS(cmd.Process.Pid)
	go func() {
		err := cmd.Wait()
		n.mu.Lock()
		n.waitErr = err
		n.state = cmd.ProcessState
		if counters := readDiskCounters(cmd.Process.Pid); counters.ok {
			n.diskEnd = counters
		}
		n.mu.Unlock()
		close(n.stopSample)
		_ = logFile.Close()
		close(n.done)
	}()
	return nil
}

func (n *node) stop(deployment Deployment) ProcessMetrics {
	n.mu.Lock()
	cmd, done := n.cmd, n.done
	n.mu.Unlock()
	if cmd == nil {
		return ProcessMetrics{Deployment: deployment, NodeID: n.id, Missing: []string{"process was not running"}}
	}
	if counters := readDiskCounters(cmd.Process.Pid); counters.ok {
		n.mu.Lock()
		n.diskEnd = counters
		n.mu.Unlock()
	}
	if cmd.Process != nil {
		_ = cmd.Process.Signal(os.Interrupt)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-done
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	metric := ProcessMetrics{Deployment: deployment, NodeID: n.id, PID: cmd.Process.Pid, PeakRSSBytes: n.peakRSS}
	if n.state != nil {
		metric.CPUTimeMS = millis(n.state.UserTime() + n.state.SystemTime())
		if peak := processStatePeakRSS(n.state); peak > metric.PeakRSSBytes {
			metric.PeakRSSBytes = peak
		}
	} else {
		metric.Missing = append(metric.Missing, "process CPU time")
	}
	if metric.PeakRSSBytes == 0 {
		metric.Missing = append(metric.Missing, "peak RSS")
	}
	if n.diskStart.ok && n.diskEnd.ok {
		metric.DiskReadBytes = subtractCounter(n.diskEnd.readBytes, n.diskStart.readBytes)
		metric.DiskWriteBytes = subtractCounter(n.diskEnd.writeBytes, n.diskStart.writeBytes)
		metric.Missing = append(metric.Missing, "per-process disk operation counts unavailable on this platform")
	} else {
		metric.Missing = append(metric.Missing, "per-process disk bytes/operations unavailable on this platform")
	}
	n.cmd = nil
	n.done = nil
	n.bootstrap = false
	n.joinAddr = ""
	return metric
}

func (n *node) sampleRSS(pid int) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if rss := processRSS(pid); rss > 0 {
			n.mu.Lock()
			if rss > n.peakRSS {
				n.peakRSS = rss
			}
			n.mu.Unlock()
		}
		if counters := readDiskCounters(pid); counters.ok {
			n.mu.Lock()
			n.diskEnd = counters
			n.mu.Unlock()
		}
		select {
		case <-n.stopSample:
			return
		case <-ticker.C:
		}
	}
}

func (c *cluster) stopNode(index int) error {
	if index < 0 || index >= len(c.nodes) {
		return fmt.Errorf("node index %d out of range", index)
	}
	c.metrics = append(c.metrics, c.nodes[index].stop(c.deployment))
	return nil
}

func (c *cluster) restartNode(ctx context.Context, index int) error {
	if index < 0 || index >= len(c.nodes) {
		return fmt.Errorf("node index %d out of range", index)
	}
	n := c.nodes[index]
	if err := n.start(ctx, c.deployment); err != nil {
		return err
	}
	return waitForRESP(n.respAddr, 15*time.Second)
}

func (c *cluster) stopAll() error {
	for i := range c.nodes {
		if c.nodes[i].cmd != nil {
			c.metrics = append(c.metrics, c.nodes[i].stop(c.deployment))
		}
	}
	return nil
}

func (c *cluster) cleanupData() {
	if c.keepData {
		return
	}
	clean := filepath.Clean(c.dataRoot)
	temp := filepath.Clean(os.TempDir())
	if filepath.Dir(clean) == temp && strings.HasPrefix(filepath.Base(clean), "skiffdb-bench-data-") {
		_ = os.RemoveAll(clean)
	}
}

func reservePorts(count int) ([]string, error) {
	listeners := make([]net.Listener, 0, count)
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	ports := make([]string, 0, count)
	seen := map[string]bool{}
	for len(ports) < count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		address := listener.Addr().String()
		if seen[address] {
			_ = listener.Close()
			continue
		}
		seen[address] = true
		listeners = append(listeners, listener)
		ports = append(ports, address)
	}
	return ports, nil
}

func waitForRESP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := dialRESP(addr, 200*time.Millisecond)
		if err == nil {
			response, commandErr := client.command("PING")
			client.close()
			if commandErr == nil && strings.HasPrefix(response, "+PONG") {
				return nil
			}
			lastErr = commandErr
		} else {
			lastErr = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("RESP endpoint %s not ready: %v", addr, lastErr)
}

func waitForWritable(nodes []*node, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.cmd == nil {
				continue
			}
			client, err := dialRESP(n.respAddr, 200*time.Millisecond)
			if err != nil {
				continue
			}
			response, err := client.command("SET", "bench:leader-probe", strconv.FormatInt(time.Now().UnixNano(), 10))
			client.close()
			if err == nil && strings.HasPrefix(response, "+OK") {
				return nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("timed out waiting for a writable Raft leader")
}

func findLeader(nodes []*node) (int, error) {
	for i, n := range nodes {
		if n.cmd == nil {
			continue
		}
		client, err := dialRESP(n.respAddr, 250*time.Millisecond)
		if err != nil {
			continue
		}
		response, err := client.command("SET", "bench:leader-probe", strconv.FormatInt(time.Now().UnixNano(), 10))
		client.close()
		if err == nil && strings.HasPrefix(response, "+OK") {
			return i, nil
		}
	}
	return -1, errors.New("no writable leader found")
}

func commandAt(addr string, args ...string) (string, error) {
	client, err := dialRESP(addr, time.Second)
	if err != nil {
		return "", err
	}
	defer client.close()
	return client.command(args...)
}

func directorySize(root string) uint64 {
	var total uint64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		if info, statErr := entry.Info(); statErr == nil {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}

func (n *node) snapshotSize() uint64 {
	return directorySize(filepath.Join(n.dataDir, "raft", n.id, "snapshots"))
}

func processRSS(pid int) uint64 {
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	kib, err := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64)
	if err != nil {
		return 0
	}
	return kib * 1024
}

func processStatePeakRSS(state *os.ProcessState) uint64 {
	if state == nil || state.SysUsage() == nil {
		return 0
	}
	value := reflect.ValueOf(state.SysUsage())
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return 0
	}
	field := value.FieldByName("Maxrss")
	if !field.IsValid() {
		return 0
	}
	var rss uint64
	if field.CanInt() && field.Int() > 0 {
		rss = uint64(field.Int())
	} else if field.CanUint() {
		rss = field.Uint()
	}
	// Linux reports ru_maxrss in KiB; Darwin reports bytes.
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	return rss
}

func readDiskCounters(pid int) diskCounters {
	file, err := os.Open(filepath.Join("/proc", strconv.Itoa(pid), "io"))
	if err != nil {
		return diskCounters{}
	}
	defer file.Close()
	var counters diskCounters
	var foundRead, foundWrite bool
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "read_bytes:":
			counters.readBytes = value
			foundRead = true
		case "write_bytes:":
			counters.writeBytes = value
			foundWrite = true
		}
	}
	counters.ok = scanner.Err() == nil && foundRead && foundWrite
	return counters
}

func subtractCounter(end, start uint64) uint64 {
	if end < start {
		return 0
	}
	return end - start
}
