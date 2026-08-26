package harness

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

type recordedOp struct {
	op        string
	latencyUS float64
	finished  time.Duration
	success   bool
}

type hookTiming struct {
	start time.Duration
	end   time.Duration
	err   error
}

func executeWorkload(ctx context.Context, addr string, deployment Deployment, workload Workload, opts Options, hookAt time.Duration, hook func() error) (WorkloadResult, hookTiming, error) {
	result := WorkloadResult{
		Deployment: deployment, Workload: workload, WarmupMS: millis(opts.Warmup),
		Concurrency: opts.Concurrency, Pipeline: opts.PipelineDepth, Seed: opts.Seed,
	}
	value := strings.Repeat("v", workload.ValueSize)
	if err := preloadKeys(addr, value); err != nil {
		return result, hookTiming{}, fmt.Errorf("preload keys: %w", err)
	}
	if opts.Warmup > 0 {
		warmCtx, cancel := context.WithTimeout(ctx, opts.Warmup)
		_, _ = runWorkloadPhase(warmCtx, addr, workload, value, opts, time.Now())
		cancel()
	}

	start := time.Now()
	phaseCtx, cancel := context.WithTimeout(ctx, opts.Duration)
	defer cancel()
	var timing hookTiming
	var hookDone chan struct{}
	if hook != nil {
		hookDone = make(chan struct{})
		go func() {
			defer close(hookDone)
			timer := time.NewTimer(hookAt)
			defer timer.Stop()
			select {
			case <-phaseCtx.Done():
				return
			case <-timer.C:
			}
			timing.start = time.Since(start)
			timing.err = hook()
			timing.end = time.Since(start)
		}()
	}
	records, phaseErr := runWorkloadPhase(phaseCtx, addr, workload, value, opts, start)
	if hookDone != nil {
		<-hookDone
	}
	elapsed := time.Since(start)
	result.DurationMS = millis(elapsed)
	result.Operations = summarizeOperations(records)
	for _, record := range records {
		if record.success {
			result.SuccessCount++
		} else {
			result.ErrorCount++
		}
	}
	if elapsed > 0 {
		result.Throughput = float64(result.SuccessCount) / elapsed.Seconds()
	}
	result.Buckets = summarizeIntervals(records, 100*time.Millisecond, elapsed)
	if ctx.Err() != nil {
		return result, timing, ctx.Err()
	}
	return result, timing, phaseErr
}

func preloadKeys(addr, value string) error {
	client, err := dialRESP(addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer client.close()
	for i := 0; i < 256; i++ {
		response, err := client.command("SET", fmt.Sprintf("bench:%d", i), value)
		if err != nil {
			return err
		}
		if !responseOK(response) {
			return fmt.Errorf("SET returned %q", strings.TrimSpace(response))
		}
	}
	return nil
}

func runWorkloadPhase(ctx context.Context, addr string, workload Workload, value string, opts Options, phaseStart time.Time) ([]recordedOp, error) {
	var wg sync.WaitGroup
	results := make(chan []recordedOp, opts.Concurrency)
	errorsCh := make(chan error, opts.Concurrency)
	for worker := 0; worker < opts.Concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(opts.Seed + int64(worker)*1_000_003))
			client, err := dialRESP(addr, 2*time.Second)
			if err != nil {
				errorsCh <- err
				return
			}
			defer client.close()
			local := make([]recordedOp, 0, 4096)
			for ctx.Err() == nil {
				batch := make([]struct {
					op    string
					start time.Time
				}, 0, opts.PipelineDepth)
				_ = client.conn.SetDeadline(time.Now().Add(10 * time.Second))
				for i := 0; i < opts.PipelineDepth; i++ {
					isGet := rng.Intn(100) < workload.GetPercent
					key := fmt.Sprintf("bench:%d", rng.Intn(256))
					started := time.Now()
					var writeErr error
					if isGet {
						writeErr = writeCommand(client.w, "GET", key)
						batch = append(batch, struct {
							op    string
							start time.Time
						}{"GET", started})
					} else {
						writeErr = writeCommand(client.w, "SET", key, value)
						batch = append(batch, struct {
							op    string
							start time.Time
						}{"SET", started})
					}
					if writeErr != nil {
						errorsCh <- writeErr
						results <- local
						return
					}
				}
				if err := client.w.Flush(); err != nil {
					errorsCh <- err
					results <- local
					return
				}
				for _, pending := range batch {
					response, err := readResponse(client.r)
					finished := time.Now()
					success := err == nil && responseOK(response)
					local = append(local, recordedOp{
						op: pending.op, latencyUS: float64(finished.Sub(pending.start).Nanoseconds()) / 1e3,
						finished: finished.Sub(phaseStart), success: success,
					})
					if err != nil {
						errorsCh <- err
						results <- local
						return
					}
				}
			}
			results <- local
		}(worker)
	}
	wg.Wait()
	close(results)
	close(errorsCh)
	var records []recordedOp
	for local := range results {
		records = append(records, local...)
	}
	var firstErr error
	for err := range errorsCh {
		if firstErr == nil {
			firstErr = err
		}
	}
	// Deadline errors at the phase boundary are expected and their operation is
	// already counted as an error. Other errors indicate a failed workload.
	if ctx.Err() != nil {
		firstErr = nil
	}
	return records, firstErr
}

func summarizeOperations(records []recordedOp) map[string]OpMetrics {
	latencies := map[string][]float64{"GET": {}, "SET": {}}
	for _, record := range records {
		if record.success {
			latencies[record.op] = append(latencies[record.op], record.latencyUS)
		}
	}
	result := make(map[string]OpMetrics, len(latencies))
	for op, values := range latencies {
		sort.Float64s(values)
		result[op] = OpMetrics{Count: uint64(len(values)), P50US: percentile(values, .50), P95US: percentile(values, .95), P99US: percentile(values, .99), P999US: percentile(values, .999)}
	}
	return result
}

func summarizeIntervals(records []recordedOp, width, total time.Duration) []IntervalMetrics {
	if width <= 0 || total <= 0 {
		return nil
	}
	count := int(math.Ceil(float64(total) / float64(width)))
	values := make([][]float64, count)
	for _, record := range records {
		index := int(record.finished / width)
		if index >= count {
			index = count - 1
		}
		if index >= 0 && record.success {
			values[index] = append(values[index], record.latencyUS)
		}
	}
	result := make([]IntervalMetrics, count)
	for i := range values {
		sort.Float64s(values[i])
		start := time.Duration(i) * width
		end := start + width
		if end > total {
			end = total
		}
		duration := end - start
		result[i] = IntervalMetrics{StartMS: millis(start), EndMS: millis(end), Operations: uint64(len(values[i])), P99US: percentile(values[i], .99)}
		if duration > 0 {
			result[i].Throughput = float64(len(values[i])) / duration.Seconds()
		}
	}
	return result
}

func percentile(sorted []float64, quantile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(quantile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func millis(duration time.Duration) float64 { return float64(duration.Nanoseconds()) / 1e6 }
