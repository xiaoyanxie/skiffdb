package resp

import (
	"fmt"
	"strings"
)

type RedisErrorCode string

const (
	ErrGeneric     RedisErrorCode = "ERROR"
	ErrWrongType   RedisErrorCode = "WRONGTYPE"
	ErrNoAuth      RedisErrorCode = "NOAUTH"
	ErrReadOnly    RedisErrorCode = "READONLY"
	ErrBusy        RedisErrorCode = "BUSY"
	ErrNoScript    RedisErrorCode = "NOSCRIPT"
	ErrOOM         RedisErrorCode = "OOM"
	ErrExecAbort   RedisErrorCode = "EXECABORT"
	ErrLoading     RedisErrorCode = "LOADING"
	ErrClusterDown RedisErrorCode = "CLUSTERDOWN"
)

// RESP Response format:
//   Type	             Prefix	Example
//   Simple String	     +	    +OK\r\n
//   Error	             -	    -ERROR msg\r\n
//   Integer	         :	    :1000\r\n
//   Bulk String	     $	    $6\r\nfoobar\r\n
//   Array	             *	    *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
//   Null Bulk String    $-1    $-1\r\n
//   Null Array	         *-1    *-1\r\n

// BuildSimpleString
// Type	             Prefix	Example
// Simple String	     +	    +OK\r\n
func BuildSimpleString(value string) string {
	return fmt.Sprintf("+%s\r\n", value)
}

// BuildErrorMsg
// Type	             Prefix	Example
// Error	         -	    -code msg\r\n
func BuildErrorMsg(code RedisErrorCode, msg string) string {
	return fmt.Sprintf("-%s %s\r\n", code, msg)
}

// BuildBulkString
// Type	             Prefix	Example
// Bulk String	     $	    $6\r\nfoobar\r\n
// Null Bulk String  $-1    $-1\r\n
func BuildBulkString(value *string) string {
	if value == nil {
		return "$-1\r\n"
	}
	ret := *value
	return fmt.Sprintf("$%d\r\n%s\r\n", len(ret), ret)
}

// BuildInteger
// Type	             Prefix	Example
// Integer	         :	    :1000\r\n
func BuildInteger(value int) string {
	return fmt.Sprintf(":%d\r\n", value)
}

// BuildArray
// Type	             Prefix	Example
// Array	         *	    *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
// Null Array	     *-1    *-1\r\n
func BuildArray(value *[]string) string {
	if value == nil {
		return "*-1\r\n"
	}
	ret := *value
	var b strings.Builder
	b.WriteString(fmt.Sprintf("*%d\r\n", len(ret)))
	for _, val := range ret {
		b.WriteString(BuildBulkString(&val))
	}
	return b.String()
}

var Ok = BuildSimpleString("OK")
var Pong = BuildSimpleString("PONG")
var ErrNoCommandSpecified = BuildErrorMsg(ErrGeneric, "No command specified")
var ErrCommandFormatError = BuildErrorMsg(ErrGeneric, "Command format error")
var ErrWrongDataType = BuildErrorMsg(ErrWrongType, "Wrong data type")
var ErrCommandUnsupported = BuildErrorMsg(ErrGeneric, "Command Unsupported")
var ErrInternal = BuildErrorMsg(ErrGeneric, "System error. Please try again.")
