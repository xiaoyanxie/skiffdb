package main

import (
    "bufio"
    "flag"
    "io"
    "kvdb/src/core"
    "kvdb/src/resp"
    "log"
    "net"
    "strings"
)

const InvalidCommand = "-ERROR Invalid command\r\n"

func main() {
    // Optional Raft flags
    enableRaft := flag.Bool("raft", false, "enable Hashicorp Raft replication")
    nodeID := flag.String("node-id", "node1", "unique Raft node ID")
    raftAddr := flag.String("raft-addr", "127.0.0.1:7000", "Raft TCP bind address (host:port)")
    bootstrap := flag.String("bootstrap-peers", "", "Bootstrap peers as 'id1=addr1,id2=addr2'")
    listenAddr := flag.String("addr", ":6379", "Server listen address")
    flag.Parse()

    if *enableRaft {
        peers := parsePeers(*bootstrap)
        if err := core.StartRaft(*nodeID, *raftAddr, peers); err != nil {
            log.Fatalf("failed to start raft: %v", err)
        }
        log.Printf("Raft enabled at %s as %s", *raftAddr, *nodeID)
    }

    ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer func(ln net.Listener) {
		err := ln.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(ln)

    log.Printf("Server listening on %s", *listenAddr)
	for {
		conn, err := ln.Accept()
		log.Println("Accepted connection from:", conn.RemoteAddr())
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Println("Failed to close connection:", err)
		}
	}(conn)

	reader := bufio.NewReader(conn)
	parserContext := resp.InitParserContext()
	for {
		err := resp.ParseRESPCommand(reader, parserContext)
		cmdArgs := parserContext.CmdArgs
		parserContext.Reset()

		if err != nil {
			if err == io.EOF {
				log.Println("Client closed connection")
				return
			}
			log.Println(err)
			writeResponse(conn, InvalidCommand)
			continue
		}

		ret := core.ExecuteCmd(cmdArgs)
		if err != nil {
			log.Println(err)
			writeResponse(conn, InvalidCommand)
			continue
		}
		writeResponse(conn, ret)
	}
}

func writeResponse(conn net.Conn, result string) {
	_, err := conn.Write([]byte(result))
	if err != nil && !isBrokenPipe(err) {
		log.Println("Connection error:", err)
	}
}

func isBrokenPipe(err error) bool {
    return strings.Contains(err.Error(), "broken pipe") || strings.Contains(err.Error(), "connection reset")
}

func parsePeers(s string) map[string]string {
    res := map[string]string{}
    if strings.TrimSpace(s) == "" {
        return res
    }
    parts := strings.Split(s, ",")
    for _, p := range parts {
        p = strings.TrimSpace(p)
        kv := strings.SplitN(p, "=", 2)
        if len(kv) != 2 {
            continue
        }
        id := strings.TrimSpace(kv[0])
        addr := strings.TrimSpace(kv[1])
        if id != "" && addr != "" {
            res[id] = addr
        }
    }
    return res
}
