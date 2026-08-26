package core

import (
	"fmt"
	"log"
	"skiffdb/src/resp"
	"strconv"
	"strings"
)

var memdb = MemDB{}

func init() {
	memdb.Init()
}

func ResetMemDB() {
	memdb.Init()
}

// Cmd Operation represents a replicated command in the Raft log.
type Cmd struct {
	Op   string   `json:"op"`
	Args []string `json:"args,omitempty"`
}

func (cmd *Cmd) ToString() string {
	return fmt.Sprintf("RESP Command: Op=%s, Args=%v", cmd.Op, cmd.Args)
}

func (cmd *Cmd) IsWriteOp() bool {
	switch cmd.Op {
	case "SET", "DEL", "INCR", "DECR", "INCRBY":
		return true
	default:
		return false
	}
}

func BuildCmd(args []string) (*Cmd, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("invalid command args %v", args)
	}
	if len(args) == 1 {
		return &Cmd{
			Op:   strings.ToUpper(args[0]),
			Args: []string{},
		}, nil
	} else {
		return &Cmd{
			Op:   strings.ToUpper(args[0]),
			Args: args[1:],
		}, nil
	}
}

// ExecuteLocally applies the operation directly to the local DB (no Raft).
func ExecuteLocally(cmd *Cmd) string {
	return executeOnDB(&memdb, cmd)
}

func executeOnDB(db *MemDB, cmd *Cmd) string {
	switch cmd.Op {
	case "PING":
		if len(cmd.Args) == 0 {
			return resp.Pong
		}
		return resp.BuildBulkString(&cmd.Args[0])
	case "SET":
		if len(cmd.Args) != 2 {
			return resp.ErrCommandFormatError
		}
		db.Set(cmd.Args[0], cmd.Args[1])
		return resp.Ok
	case "GET":
		if len(cmd.Args) != 1 {
			return resp.ErrCommandFormatError
		}
		return resp.BuildBulkString(db.Get(cmd.Args[0]))
	case "DEL":
		if len(cmd.Args) != 1 {
			return resp.ErrCommandFormatError
		}
		db.Delete(cmd.Args[0])
		return resp.Ok
	case "INCR":
		if len(cmd.Args) != 1 {
			return resp.ErrCommandFormatError
		}
		err := db.Incr(cmd.Args[0], 1)
		if err != nil {
			return resp.ErrWrongDataType
		}
		return resp.Ok
	case "DECR":
		if len(cmd.Args) != 1 {
			return resp.ErrCommandFormatError
		}
		err := db.Incr(cmd.Args[0], -1)
		if err != nil {
			return resp.ErrWrongDataType
		}
		return resp.Ok
	case "INCRBY":
		if len(cmd.Args) != 2 {
			return resp.ErrCommandFormatError
		}
		delta, err := strconv.Atoi(cmd.Args[1])
		if err != nil {
			return resp.ErrCommandFormatError
		}
		err = db.Incr(cmd.Args[0], delta)
		if err != nil {
			return resp.ErrWrongDataType
		}
		return resp.Ok
	case "EXISTS":
		if len(cmd.Args) < 1 {
			return resp.ErrCommandFormatError
		}
		cnt := db.CountKeys(cmd.Args)
		return resp.BuildInteger(cnt)

	/*********** Time-to-Live (TTL) Commands ***********/
	//	Command		Description
	//	EXPIRE		Set a key with a expiration time
	//	TTL			Get the key's remaining expiration time
	case "TTL":
		// TODO: implement TTL
	case "EXPIRE":
		// TODO: implement EXPIRE

	/****************** Hash commands  *****************/
	//	Command		Description
	//	HSET		Used for objects/maps (e.g., user profiles)
	//	HGET		Pair to HSET
	case "HSET":
		// TODO: implement HSET
	case "HGET":
		// TODO: implement HGET

	/****************** List commands  *****************/
	//	Command		Description
	//	LPUSH		Used for queues, logs, streams
	//	LRANGE		Read from lists
	case "LPUSH":
		// TODO: implement LPUSH
	case "LRANGE":
		// TODO: implement LRANGE

	/****************  ZSET operations  ****************/
	//	Command					Description
	//	ZADD					Add/update members with scores
	//	ZSCORE					Get score of a member
	//	ZRANGE					Get members by rank (ascending order)
	//	ZREVRANGE				Get members by rank (descending order)
	//	ZRANGEBYSCORE			Get members by score range (low → high)
	//	ZRANK					Get rank (index) of a member
	//	ZREM					Remove one or more members
	//	ZINCRBY					Increment the score of a member
	// TODO: implement ZSET operations

	/******************* Persistence *******************/
	//	Command					Description
	//	SAVE					Manual backup, used in testing/debugging
	//	FLUSHDB					Used in dev/testing to clear data
	case "SAVE":
		if len(cmd.Args) != 0 {
			return resp.ErrCommandFormatError
		}
		if err := SaveSnapshot(); err != nil {
			log.Printf("SAVE failed: %v", err)
			return resp.BuildErrorMsg(resp.ErrGeneric, fmt.Sprintf("Snapshot failed: %v", err))
		}
		return resp.Ok
	case "FLUSHDB":
		// TODO: implement FLUSHDB
	}
	return resp.ErrCommandUnsupported
}
