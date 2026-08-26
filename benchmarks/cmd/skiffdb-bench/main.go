package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
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
		flags.IntVar(&options.KeyCount, "keys", 256, "number of uniformly distributed benchmark keys")
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
	case "remote":
		flags := flag.NewFlagSet("remote", flag.ExitOnError)
		var options harness.Options
		targets := flags.String("targets", "", "comma-separated RESP host:port targets")
		deployment := flags.String("deployment", string(harness.DurableThree), "deployment label: in-memory-single, durable-single-voter, or durable-three-voter")
		flags.StringVar(&options.Profile, "profile", "smoke", "benchmark profile: smoke or full")
		flags.StringVar(&options.ResultsRoot, "results", "benchmarks/results", "result directory root")
		flags.DurationVar(&options.Duration, "duration", 2*time.Second, "measured duration per workload")
		flags.DurationVar(&options.Warmup, "warmup", 500*time.Millisecond, "warm-up duration per workload")
		flags.IntVar(&options.Concurrency, "concurrency", 4, "parallel client connections")
		flags.IntVar(&options.PipelineDepth, "pipeline", 1, "commands in each pipeline")
		flags.IntVar(&options.KeyCount, "keys", 256, "number of uniformly distributed benchmark keys")
		flags.Int64Var(&options.Seed, "seed", 1, "deterministic random seed")
		_ = flags.Parse(os.Args[2:])
		bundle, err := harness.RunRemote(ctx, options, harness.Deployment(*deployment), strings.Split(*targets, ","))
		if bundle != nil {
			fmt.Printf("results: %s\n", bundle.Dir)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "remote benchmark failed:", err)
			os.Exit(1)
		}
	case "leader":
		flags := flag.NewFlagSet("leader", flag.ExitOnError)
		targets := flags.String("targets", "", "comma-separated RESP host:port targets")
		deployment := flags.String("deployment", string(harness.DurableThree), "deployment label")
		_ = flags.Parse(os.Args[2:])
		target, index, err := harness.FindWritableTarget(strings.Split(*targets, ","), harness.Deployment(*deployment))
		if err != nil {
			fmt.Fprintln(os.Stderr, "leader probe failed:", err)
			os.Exit(1)
		}
		fmt.Printf("%d %s\n", index, target)
	case "write":
		flags := flag.NewFlagSet("write", flag.ExitOnError)
		targets := flags.String("targets", "", "comma-separated RESP host:port targets")
		deployment := flags.String("deployment", string(harness.DurableThree), "deployment label")
		key := flags.String("key", "", "marker key")
		value := flags.String("value", "", "marker value")
		_ = flags.Parse(os.Args[2:])
		if *key == "" {
			fmt.Fprintln(os.Stderr, "write requires --key")
			os.Exit(2)
		}
		target, index, err := harness.SetOnWritableTarget(strings.Split(*targets, ","), harness.Deployment(*deployment), *key, *value)
		if err != nil {
			fmt.Fprintln(os.Stderr, "remote write failed:", err)
			os.Exit(1)
		}
		fmt.Printf("%d %s\n", index, target)
	case "wait-value":
		flags := flag.NewFlagSet("wait-value", flag.ExitOnError)
		target := flags.String("target", "", "RESP host:port target")
		key := flags.String("key", "", "marker key")
		value := flags.String("value", "", "expected marker value")
		timeout := flags.Duration("timeout", 30*time.Second, "maximum wait duration")
		_ = flags.Parse(os.Args[2:])
		if *target == "" || *key == "" {
			fmt.Fprintln(os.Stderr, "wait-value requires --target and --key")
			os.Exit(2)
		}
		if err := harness.WaitForRemoteValue(*target, *key, *value, *timeout); err != nil {
			fmt.Fprintln(os.Stderr, "wait for remote value failed:", err)
			os.Exit(1)
		}
		fmt.Printf("value observed at %s\n", *target)
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

func usage() {
	fmt.Fprintln(os.Stderr, "usage: skiffdb-bench <run|remote|leader|write|wait-value|compare> [options]")
}
