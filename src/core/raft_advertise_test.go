package core

import "testing"

func TestNormalizeRaftAdvertiseAddrPreservesStableHostname(t *testing.T) {
	got, err := normalizeRaftAdvertiseAddr("localhost:17000")
	if err != nil {
		t.Fatalf("normalizeRaftAdvertiseAddr() error = %v", err)
	}
	if got != "localhost:17000" {
		t.Fatalf("normalizeRaftAdvertiseAddr() = %q, want stable hostname", got)
	}
}

func TestNormalizeRaftAdvertiseAddrRejectsUnroutableBindAddress(t *testing.T) {
	if _, err := normalizeRaftAdvertiseAddr("0.0.0.0:17000"); err == nil {
		t.Fatal("normalizeRaftAdvertiseAddr() accepted an unspecified address")
	}
}
