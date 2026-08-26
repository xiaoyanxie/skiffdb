package harness

import (
	"fmt"
	"time"
)

func ResolveProfile(name string) (Profile, error) {
	mixes := []struct {
		name string
		get  int
	}{
		{"get100", 100},
		{"set100", 0},
		{"get95-set5", 95},
		{"get50-set50", 50},
	}
	sizes := []int{64, 1024, 16 * 1024}
	full := Profile{
		Name:        "full",
		Deployments: []Deployment{InMemorySingle, DurableSingle, DurableThree},
		Scenarios:   true,
	}
	for _, mix := range mixes {
		for _, size := range sizes {
			full.Workloads = append(full.Workloads, Workload{
				Name: mix.name, GetPercent: mix.get, SetPercent: 100 - mix.get, ValueSize: size,
			})
		}
	}
	switch name {
	case "full":
		return full, nil
	case "smoke":
		return Profile{
			Name:        "smoke",
			Deployments: full.Deployments,
			Workloads:   []Workload{{Name: "get95-set5", GetPercent: 95, SetPercent: 5, ValueSize: 64}},
			Scenarios:   true,
		}, nil
	default:
		return Profile{}, fmt.Errorf("unknown profile %q (want smoke or full)", name)
	}
}

func (o *Options) setDefaults() {
	if o.Profile == "" {
		o.Profile = "smoke"
	}
	if o.ResultsRoot == "" {
		o.ResultsRoot = "benchmarks/results"
	}
	if o.ServerBinary == "" {
		o.ServerBinary = "./skiffdb-server"
	}
	if o.BuildCommand == "" {
		o.BuildCommand = "go build -o ./skiffdb-server ."
	}
	if o.Duration <= 0 {
		o.Duration = 2 * time.Second
	}
	if o.Warmup < 0 {
		o.Warmup = 0
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 4
	}
	if o.PipelineDepth <= 0 {
		o.PipelineDepth = 1
	}
	if o.Seed == 0 {
		o.Seed = 1
	}
}
