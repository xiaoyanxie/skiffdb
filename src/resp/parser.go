package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
)

var Separator = []byte("\r\n")

const (
	CommandStart = iota
	BulkStringLen
	BulkStringBody
	Array
)

type ParserContext struct {
	CmdArgs    []string
	index      int
	bulkStrLen int
	state      int
}

func (ctx *ParserContext) Reset() {
	ctx.bulkStrLen = -1
	ctx.CmdArgs = nil
	ctx.index = -1
	ctx.state = CommandStart
}

func (ctx *ParserContext) initCmdArgs(length int) error {
	if length < 0 {
		return fmt.Errorf("invalid array length: %d", length)
	}
	ctx.CmdArgs = make([]string, length)
	ctx.index = 0
	return nil
}

func (ctx *ParserContext) setArg(arg string) error {
	if ctx.CmdArgs == nil {
		return fmt.Errorf("found bulkstring argument '%s' at %d. expecting an array", arg, ctx.index)
	}
	if ctx.CmdArgs == nil || ctx.index >= len(ctx.CmdArgs) {
		return fmt.Errorf("expecting array length > %d, found argument '%s' at %d", ctx.index, arg, ctx.index)
	}
	ctx.CmdArgs[ctx.index] = arg
	ctx.index++
	return nil
}

func InitParserContext() *ParserContext {
	return &ParserContext{state: CommandStart}
}

func ParseRESPCommand(reader *bufio.Reader, ctx *ParserContext) error {
	// RESP Request format:
	// A valid request cmd will always have suffix "\r\n", one valid command consists multiple `cmd`s.
	// A valid RESP command follows this format:
	//   Type	             Prefix	Example
	//   Bulk String	     $	    $6\r\nfoobar\r\n
	//   Array	             *	    *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
	switch ctx.state {
	case CommandStart:
		cmd, err := readNextCmd(reader)
		if err != nil {
			log.Println("Failed to read command:", err)
			return err
		}
		if string(cmd) == "PING" {
			_ = ctx.initCmdArgs(1)
			_ = ctx.setArg("PING")
			return nil
		}
		err = readCommandStart(cmd, ctx)
		if err != nil {
			log.Println("Failed to read command:", err)
			return err
		}
		return ParseRESPCommand(reader, ctx)
	case BulkStringLen:
		cmd, err := readNextCmd(reader)
		if err != nil {
			log.Println("Failed to read command:", err)
			return err
		}
		err = readCommandStart(cmd, ctx)
		if err != nil || ctx.bulkStrLen <= 0 || ctx.state != BulkStringBody {
			log.Println("Failed to read bulk string len:", err)
			return err
		}
		return ParseRESPCommand(reader, ctx)
	case BulkStringBody:
		cmd, err := readNextCmdFixedLength(reader, ctx.bulkStrLen)
		if err != nil {
			log.Println("Failed to read command:", err)
			return err
		}
		str := string(cmd)
		err = ctx.setArg(str)
		if err != nil {
			log.Println("Failed to set argument:", err)
			return err
		}
		return nil
	case Array:
		for i := 0; i < len(ctx.CmdArgs); i++ {
			ctx.state = BulkStringLen
			err := ParseRESPCommand(reader, ctx)
			if err != nil {
				log.Println("Failed to read bulk string in array:", err)
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown state: %d", ctx.state)
	}
}

func readNextCmd(reader *bufio.Reader) ([]byte, error) {
	cmd, err := reader.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(cmd) < 4 {
		return nil, fmt.Errorf("command too short: %d", len(cmd))
	}
	if !bytes.HasSuffix(cmd, Separator) {
		return nil, fmt.Errorf("expecting separator: \\r\\n")
	}
	return bytes.TrimSuffix(cmd, Separator), nil
}

func readNextCmdFixedLength(reader *bufio.Reader, length int) ([]byte, error) {
	cmd := make([]byte, length+2)
	_, err := io.ReadFull(reader, cmd)
	if err != nil {
		return nil, err
	}
	if len(cmd) < 4 {
		return nil, fmt.Errorf("command too short: %d", len(cmd))
	}
	if !bytes.HasSuffix(cmd, Separator) {
		return nil, fmt.Errorf("expecting separator: \\r\\n")
	}
	return bytes.TrimSuffix(cmd, Separator), nil
}

func readCommandStart(cmd []byte, ctx *ParserContext) error {
	cmdType := cmd[0]
	switch cmdType {
	case '$': // Bulk String
		length, err := getLengthInHeader(cmd)
		if err != nil {
			return fmt.Errorf("failed to read command length: %v", err)
		}
		ctx.bulkStrLen = length
		ctx.state = BulkStringBody
		return nil
	case '*': // Array
		length, err := getLengthInHeader(cmd)
		if err != nil {
			return fmt.Errorf("failed to read command length: %v", err)
		}
		err = ctx.initCmdArgs(length)
		if err != nil {
			return err
		}
		ctx.state = Array
		return nil
	default:
		return fmt.Errorf("unknown command type prefix: %v", string(cmdType))
	}
}

func getLengthInHeader(cmd []byte) (int, error) {
	length, err := strconv.Atoi(string(cmd[1:]))
	if err != nil {
		return -1, err
	}
	return length, err
}
