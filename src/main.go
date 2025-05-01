package main

import (
	"bufio"
	"fmt"
	"kvdb/src/parser"
	"log"
	"net"
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

	fmt.Println("Server listening on :6379")
	for {
		conn, err := ln.Accept()
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
	parserContext := parser.InitParserContext()
	for {
		err := parser.ParseRESPCommand(reader, parserContext)
		if err != nil {
			_, err := conn.Write([]byte(InvalidCommand))
			if err != nil {
				log.Println("Connection error:", err)
				return
			}
		}
		fmt.Println(parserContext.CmdArgs)
		parserContext.Reset()
	}
}
