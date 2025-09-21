package test

import (
	"bufio"
	"log"
	"skiffdb/src/resp"
	"strings"
	"testing"
)

func TestParseRESPCommand(t *testing.T) {
	type args struct {
		reader *bufio.Reader
		ctx    *resp.ParserContext
	}
	ctx := resp.InitParserContext()
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		expected []string
	}{
		// TODO: Add test cases.
		{
			name: "valid PING command",
			args: args{
				reader: bufio.NewReader(strings.NewReader(resp.BuildArray(&[]string{"PING", "test"}))),
				ctx:    ctx,
			},
			wantErr:  false,
			expected: []string{"PING", "test"},
		},
		{
			name: "valid SET command 1",
			args: args{
				reader: bufio.NewReader(strings.NewReader(resp.BuildArray(&[]string{"SET", "key", "value"}))),
				ctx:    ctx,
			},
			wantErr:  false,
			expected: []string{"SET", "key", "value"},
		},
		{
			name: "valid command with multiline BulkString",
			args: args{
				reader: bufio.NewReader(strings.NewReader(resp.BuildArray(&[]string{"\nSET", "\nke\ny", "v\ral\nue\n"}))),
				ctx:    ctx,
			},
			wantErr:  false,
			expected: []string{"\nSET", "\nke\ny", "v\ral\nue\n"},
		},
		{
			name: "invalid command 1",
			args: args{
				reader: bufio.NewReader(strings.NewReader("*3\r\n$4\r\n\nSET\r\nabc\r\n")),
				ctx:    ctx,
			},
			wantErr: true,
		},
		{
			name: "invalid command 2",
			args: args{
				reader: bufio.NewReader(strings.NewReader("$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")),
				ctx:    ctx,
			},
			wantErr: true,
		},
		{
			name: "valid SET command 2",
			args: args{
				reader: bufio.NewReader(strings.NewReader(resp.BuildArray(&[]string{"SET", "key", "value"}))),
				ctx:    ctx,
			},
			wantErr:  false,
			expected: []string{"SET", "key", "value"},
		},
		{
			name: "valid GET command",
			args: args{
				reader: bufio.NewReader(strings.NewReader(resp.BuildArray(&[]string{"GET", "a"}))),
				ctx:    ctx,
			},
			wantErr:  false,
			expected: []string{"GET", "a"},
		},
		{
			name: "invalid GET command",
			args: args{
				reader: bufio.NewReader(strings.NewReader(resp.BuildArray(&[]string{"GET"}))),
				ctx:    ctx,
			},
			wantErr:  false,
			expected: []string{"GET"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.args.ctx.Reset()
			if err := resp.ParseRESPCommand(tt.args.reader, tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("ParseRESPCommand() error = %v, wantErr %v", err, tt.wantErr)
			}
			result := tt.args.ctx.CmdArgs
			if tt.wantErr {
				return
			}
			log.Printf("Parse result: %s\n", result)
			for i, arg := range tt.expected {
				if arg != result[i] {
					t.Errorf("ParseRESPCommand() error = %v, expected %v", result, tt.expected)
				}
			}
		})
	}
}
