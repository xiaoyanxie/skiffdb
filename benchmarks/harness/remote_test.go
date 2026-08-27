package harness

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
)

func TestRemoteDefaultsRecordOrchestrationAndKeyCount(t *testing.T) {
	options := Options{}
	options.setDefaults()
	if options.Orchestration != "local-process" {
		t.Fatalf("orchestration=%q, want local-process", options.Orchestration)
	}
	if options.KeyCount != 256 {
		t.Fatalf("key count=%d, want 256", options.KeyCount)
	}
}

func TestNormalizeTargetsValidatesAndDeduplicates(t *testing.T) {
	targets, err := normalizeTargets([]string{" 127.0.0.1:6379 ", "127.0.0.1:6379", "[::1]:6380"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(targets, ","); got != "127.0.0.1:6379,[::1]:6380" {
		t.Fatalf("targets=%q", got)
	}
	if _, err := normalizeTargets([]string{"missing-port"}); err == nil {
		t.Fatal("normalizeTargets accepted an invalid endpoint")
	}
}

func TestFindWritableTargetSelectsLeader(t *testing.T) {
	follower, stopFollower := startRESPStatusServer(t, "-ERR write not allowed\r\n")
	defer stopFollower()
	leader, stopLeader := startRESPStatusServer(t, "+OK\r\n")
	defer stopLeader()

	target, index, err := FindWritableTarget([]string{follower, leader}, DurableThree)
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 || target != leader {
		t.Fatalf("leader=(%d, %q), want (1, %q)", index, target, leader)
	}
}

func startRESPStatusServer(t *testing.T, response string) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			reader := bufio.NewReader(conn)
			header, err := reader.ReadString('\n')
			if err == nil && strings.HasPrefix(header, "*") {
				count, parseErr := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "*")))
				if parseErr == nil {
					for line := 0; line < count*2; line++ {
						_, _ = reader.ReadString('\n')
					}
					_, _ = fmt.Fprint(conn, response)
				}
			}
			_ = conn.Close()
		}
	}()
	return listener.Addr().String(), func() {
		_ = listener.Close()
		<-done
	}
}

func TestOrchestrationIdentityTreatsMissingMetadataAsLocal(t *testing.T) {
	if got := orchestrationIdentity(""); got != "local-process" {
		t.Fatalf("identity=%q", got)
	}
	if got := orchestrationIdentity("remote-target"); got != "remote-target" {
		t.Fatalf("identity=%q", got)
	}
}

func TestRemoteConfigurationRedactsTargetAddresses(t *testing.T) {
	configurations := remoteNodeConfigurations([]string{"10.0.0.12:6379"}, DurableThree)
	if len(configurations) != 1 || configurations[0].RESPAddress != "<redacted>" {
		t.Fatalf("configurations=%#v", configurations)
	}
}
