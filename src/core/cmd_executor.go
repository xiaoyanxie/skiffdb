package core

import (
	"errors"
	"fmt"
	"log"
)

//const (
//	SimpleString = iota
//	Error
//	Integer
//	BulkString
//	Array
//	NullBulkString
//	NullArray
//)

func ExecuteCmd(cmdArgs []string) (string, error) {
	// RESP Response format:
	//   Type	             Prefix	Example
	//   Simple String	     +	    +OK\r\n
	//   Error	             -	    -ERROR msg\r\n
	//   Integer	         :	    :1000\r\n
	//   Bulk String	     $	    $6\r\nfoobar\r\n
	//   Array	             *	    *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
	//   Null Bulk String    $-1    $-1\r\n
	//   Null Array	         *-1    *-1\r\n

	log.Printf("Executing command: %s\n", cmdArgs)
	if len(cmdArgs) < 1 {
		return "", errors.New("no command specified")
	}
	switch cmdArgs[0] {
	case "PING":
		if len(cmdArgs) != 1 {
			return "", fmt.Errorf("unsupported command: %s", cmdArgs)
		}
		ret := "PONG"
		return fmt.Sprintf("$%d\r\n%s\r\n", len(ret), ret), nil
	default:
		ret := "TEST_RESPONSE"
		return fmt.Sprintf("$%d\r\n%s\r\n", len(ret), ret), nil
	}
}
