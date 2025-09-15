package core

import (
    "kvdb/src/resp"
    "log"
    "strconv"
    "time"
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
		return resp.ErrNoCommandSpecified
	}
	switch cmdArgs[0] {
	/****************   Core commands   ****************/
	//	Command		Description
	//	PING		Used for connectivity check (redis-cli ping)
	//	SET			Set a key with a value
	//	GET			Get a value by key
	//	DEL			Delete a single key
	//	EXISTS		Check presence of keys
	case "PING":
		if len(cmdArgs) == 1 {
			return resp.Pong
		}
		return resp.BuildBulkString(&cmdArgs[1])
	case "SET":
		if len(cmdArgs) != 3 {
			return resp.ErrCommandFormatError
		}
		if RaftEnabled() {
			if !IsLeader() {
				return resp.BuildErrorMsg(resp.ErrReadOnly, "writes allowed only on leader")
			}
			_, err := ApplyOperation(Operation{Op: "SET", Key: cmdArgs[1], Value: cmdArgs[2]}, 5*time.Second)
			if err != nil {
				return resp.BuildErrorMsg(resp.ErrGeneric, err.Error())
			}
			return resp.Ok
		}
		memdb.Set(cmdArgs[1], cmdArgs[2])
		return resp.Ok
	case "GET":
		if len(cmdArgs) != 2 {
			return resp.ErrCommandFormatError
		}
		return resp.BuildBulkString(memdb.Get(cmdArgs[1]))
	case "DEL":
		if len(cmdArgs) != 2 {
			return resp.ErrCommandFormatError
		}
		if RaftEnabled() {
			if !IsLeader() {
				return resp.BuildErrorMsg(resp.ErrReadOnly, "writes allowed only on leader")
			}
			_, err := ApplyOperation(Operation{Op: "DEL", Key: cmdArgs[1]}, 5*time.Second)
			if err != nil {
				return resp.BuildErrorMsg(resp.ErrGeneric, err.Error())
			}
			return resp.Ok
		}
		memdb.Delete(cmdArgs[1])
		return resp.Ok
	case "EXISTS":
		if len(cmdArgs) < 2 {
			return resp.ErrCommandFormatError
		}
		cnt := memdb.CountKeys(cmdArgs[1:])
		return resp.BuildInteger(cnt)

	/**************** Numeric commands  ****************/
	//	Command		Description
	//	INCR		Used for counters, IDs
	//	DECR		Similar use case
	//	INCRBY		More general than INCR
	case "INCR":
		if len(cmdArgs) != 2 {
			return resp.ErrCommandFormatError
		}
		if RaftEnabled() {
			if !IsLeader() {
				return resp.BuildErrorMsg(resp.ErrReadOnly, "writes allowed only on leader")
			}
			_, err := ApplyOperation(Operation{Op: "INCR", Key: cmdArgs[1], Delta: 1}, 5*time.Second)
			if err != nil {
				return resp.ErrWrongDataType
			}
			return resp.Ok
		}
		err := memdb.Incr(cmdArgs[1], 1)
		if err != nil {
			return resp.ErrWrongDataType
		}
		return resp.Ok
	case "DECR":
		if len(cmdArgs) != 2 {
			return resp.ErrCommandFormatError
		}
		if RaftEnabled() {
			if !IsLeader() {
				return resp.BuildErrorMsg(resp.ErrReadOnly, "writes allowed only on leader")
			}
			_, err := ApplyOperation(Operation{Op: "INCR", Key: cmdArgs[1], Delta: -1}, 5*time.Second)
			if err != nil {
				return resp.ErrWrongDataType
			}
			return resp.Ok
		}
		err := memdb.Incr(cmdArgs[1], -1)
		if err != nil {
			return resp.ErrWrongDataType
		}
		return resp.Ok
	case "INCRBY":
		if len(cmdArgs) != 3 {
			return resp.ErrCommandFormatError
		}
		delta, err := strconv.Atoi(cmdArgs[2])
		if err != nil {
			return resp.ErrCommandFormatError
		}
		if RaftEnabled() {
			if !IsLeader() {
				return resp.BuildErrorMsg(resp.ErrReadOnly, "writes allowed only on leader")
			}
			_, err := ApplyOperation(Operation{Op: "INCR", Key: cmdArgs[1], Delta: delta}, 5*time.Second)
			if err != nil {
				return resp.ErrWrongDataType
			}
			return resp.Ok
		}
		err = memdb.Incr(cmdArgs[1], delta)
		if err != nil {
			return resp.ErrWrongDataType
		}
		return resp.Ok

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
		// Trigger a snapshot when Raft is enabled; otherwise no-op.
		if RaftEnabled() {
			f := raftNode.raft.Snapshot()
			if err := f.Error(); err != nil {
				return resp.BuildErrorMsg(resp.ErrGeneric, err.Error())
			}
		}
		return resp.Ok
	case "FLUSHDB":
		// TODO: implement FLUSHDB
	}
	return resp.ErrCommandUnsupported
}
