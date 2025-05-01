package main

import (
	"bufio"
	"fmt"
	"kvdb/src/core"
	"kvdb/src/resp"
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
	parserContext := resp.InitParserContext()
	for {
		err := resp.ParseRESPCommand(reader, parserContext)
		if err != nil {
			writeErrorResponse(conn)
			return
		}

		cmdArgs := parserContext.CmdArgs
		fmt.Println(cmdArgs)
		parserContext.Reset()

		ret, retType, err := core.ExecuteCmd(cmdArgs)
		if err != nil {
			writeErrorResponse(conn)
			return
		}
		writeResponse(conn, ret, retType)
	}
}

func writeResponse(conn net.Conn, result string, retType int) {
	// RESP Response format:
	//   Type	             Prefix	Example
	//   Simple String	     +	    +OK\r\n
	//   Error	             -	    -ERROR msg\r\n
	//   Integer	         :	    :1000\r\n
	//   Bulk String	     $	    $6\r\nfoobar\r\n
	//   Array	             *	    *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
	//   Null Bulk String    $-1    $-1\r\n
	//   Null Array	         *-1    *-1\r\n

	var retStr string
	switch retType {
	case core.BulkString:
		retStr = fmt.Sprintf("$%d\r\n%s\r\n", len(result), result)
	default:
		retStr = "TEST_RESPONSE"
	}

	_, err := conn.Write([]byte(retStr))
	if err != nil {
		log.Println("Connection error:", err)
	}
}

func writeErrorResponse(conn net.Conn) {
	_, err := conn.Write([]byte(InvalidCommand))
	if err != nil {
		log.Println("Connection error:", err)
	}
}
