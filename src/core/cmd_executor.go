package core

import (
	"kvdb/src/resp"
	"log"
)

var memdb = MemDB{}

func init() {
	memdb.Init()
}

func ResetMemDB() {
	memdb.Init()
}

func ExecuteCmd(cmdArgs []string) string {
	log.Printf("Executing command: %s\n", cmdArgs)
	if len(cmdArgs) < 1 {
		return resp.BuildErrorMsg(resp.ErrGeneric, "No command specified")
	}
	switch cmdArgs[0] {
	case "PING":
		if len(cmdArgs) == 1 {
			return resp.BuildSimpleString("PONG")
		}
		return resp.BuildBulkString(&cmdArgs[1])
	case "SET":
		if len(cmdArgs) != 3 {
			resp.BuildErrorMsg(resp.ErrGeneric, "SET expects 1 argument")
		}
		memdb.Set(cmdArgs[1], cmdArgs[2])
		return resp.BuildSimpleString("OK")
	case "GET":
		if len(cmdArgs) != 2 {
			resp.BuildErrorMsg(resp.ErrGeneric, "GET expects 1 argument")
		}
		return resp.BuildBulkString(memdb.Get(cmdArgs[1]))
	case "DEL":
		if len(cmdArgs) != 2 {
			resp.BuildErrorMsg(resp.ErrGeneric, "DEL expects 1 argument")
		}
		memdb.Delete(cmdArgs[1])
		return resp.BuildSimpleString("OK")
	case "INCR":
		if len(cmdArgs) != 2 {
			resp.BuildErrorMsg(resp.ErrGeneric, "INCR expects 1 argument")
		}
		err := memdb.Incr(cmdArgs[1])
		if err != nil {
			return resp.BuildErrorMsg(resp.ErrWrongType, err.Error())
		}
		return resp.BuildSimpleString("OK")
	case "EXISTS":
		if len(cmdArgs) < 2 {
			resp.BuildErrorMsg(resp.ErrGeneric, "EXISTS expects at least 1 argument")
		}
		cnt := memdb.CountKeys(cmdArgs[1:])
		return resp.BuildInteger(cnt)
	default:
		return resp.BuildErrorMsg(resp.ErrGeneric, "Unsupported")
	}
}
