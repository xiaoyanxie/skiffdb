package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"skiffdb/benchmarks/harness"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch os.Args[1] {
	case "run":
		flags := flag.NewFlagSet("run", flag.ExitOnError)
		var options harness.Options
		flags.StringVar(&options.Profile, "profile", "smoke", "benchmark profile: smoke or full")
		flags.StringVar(&options.ResultsRoot, "results", "benchmarks/results", "result directory root")
		flags.StringVar(&options.ServerBinary, "server", "./skiffdb-server", "SkiffDB server binary")
		flags.StringVar(&options.BuildCommand, "build-command", "go build -o ./skiffdb-server .", "server build command")
		flags.DurationVar(&options.Duration, "duration", 2*time.Second, "measured duration per workload")
		flags.DurationVar(&options.Warmup, "warmup", 500*time.Millisecond, "warm-up duration per workload")
		flags.IntVar(&options.Concurrency, "concurrency", 4, "parallel client connections")
		flags.IntVar(&options.PipelineDepth, "pipeline", 1, "commands in each pipeline")
		flags.Int64Var(&options.Seed, "seed", 1, "deterministic random seed")
		flags.BoolVar(&options.KeepData, "keep-data", false, "preserve harness-created temporary node data")
		_ = flags.Parse(os.Args[2:])
		bundle, err := harness.Run(ctx, options)
		if bundle != nil {
			fmt.Printf("results: %s\n", bundle.Dir)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "benchmark failed:", err)
			os.Exit(1)
		}
	case "compare":
		flags := flag.NewFlagSet("compare", flag.ExitOnError)
		output := flags.String("output", "", "comparison output directory (default: candidate/comparison)")
		_ = flags.Parse(os.Args[2:])
		if flags.NArg() != 2 {
			fmt.Fprintln(os.Stderr, "compare requires BASELINE_DIR CANDIDATE_DIR")
			os.Exit(2)
		}
		comparison, err := harness.Compare(flags.Arg(0), flags.Arg(1), *output)
		if err != nil {
			fmt.Fprintln(os.Stderr, "comparison failed:", err)
			os.Exit(1)
		}
		fmt.Printf("comparable: %t; metrics compared: %d\n", comparison.Comparable, len(comparison.Changes))
	default:
		usage()
		os.Exit(2)
	}
}

func usage() { fmt.Fprintln(os.Stderr, "usage: skiffdb-bench <run|compare> [options]") }
