package main

import (
	"bufio"
	"io"
	"kvdb/src/core"
	"kvdb/src/resp"
	"log"
	"net"
	"strings"
)

const InvalidCommand = "-ERROR Invalid command\r\n"

func main() {
	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	defer func(ln net.Listener) {
		err := ln.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(ln)

	log.Println("Server listening on :6379")
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
