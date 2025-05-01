package core

import (
	"errors"
	"fmt"
)

const (
	SimpleString = iota
	Error
	Integer
	BulkString
	Array
	NullBulkString
	NullArray
)

func ExecuteCmd(cmdArgs []string) (string, int, error) {
	if len(cmdArgs) < 1 {
		return "", Error, errors.New("no command specified")
	}
	switch cmdArgs[0] {
	case "PING":
		if len(cmdArgs) != 1 {
			return "", Error, fmt.Errorf("unsupported command: %s", cmdArgs)
		}
		return "PONG", BulkString, nil
	default:
		return "", Error, fmt.Errorf("unsupported command: %s", cmdArgs)
	}
}
