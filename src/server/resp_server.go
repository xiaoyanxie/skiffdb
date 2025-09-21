package server

import (
	"bufio"
	"context"
	"io"
	"kvdb/src/core"
	"kvdb/src/resp"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

func InitRESPTCPServer(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	ln, err := net.Listen("tcp", core.DBConfig.ListenAddr)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		<-ctx.Done()
		err := ln.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("database server listening at %s", ln.Addr())
	go func() {
		for {
			conn, err := ln.Accept()
			log.Println("Accepted connection from:", conn.RemoteAddr())
			if err != nil {
				log.Println("Failed to accept connection:", err)
				continue
			}
			go handleConnection(conn)
		}
	}()
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
			writeResponse(conn, resp.ErrInvalidCommand)
			continue
		}

		cmd, err := core.BuildCmd(cmdArgs)
		if err != nil {
			log.Println(err)
			writeResponse(conn, resp.ErrInvalidCommand)
			continue
		}

		ret := executeRESPCmd(cmd)
		if err != nil {
			log.Println(err)
			writeResponse(conn, resp.ErrInvalidCommand)
			continue
		}
		writeResponse(conn, ret)
	}
}

func executeRESPCmd(cmd *core.Cmd) string {
	log.Printf("Executing command: %s\n", cmd.ToString())
	if cmd == nil {
		return resp.ErrNoCommandSpecified
	}

	if cmd.IsWriteOp() && core.RaftEnabled() {
		if !core.IsLeader() {
			return resp.ErrWriteNotAllowed
		}
		return core.ApplyCmdViaRaft(cmd, 5*time.Second)
	} else {
		return core.ExecuteLocally(cmd)
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
