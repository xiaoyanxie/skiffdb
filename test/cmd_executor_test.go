package test

import (
	"kvdb/src/core"
	"kvdb/src/resp"
	"testing"
)

func TestExecuteCmd(t *testing.T) {
	type args struct {
		cmdArgs []string
	}
	var tests = []struct {
		name string
		init [][]string
		args args
		want string
	}{
		// TODO: Add test cases.
		{
			name: "SET key value",
			init: [][]string{},
			args: args{cmdArgs: []string{"SET", "a", "123a"}},
			want: resp.BuildSimpleString("OK"),
		},
		{
			name: "GET key not exist",
			init: [][]string{},
			args: args{cmdArgs: []string{"GET", "a"}},
			want: resp.BuildBulkString(nil),
		},
		{
			name: "GET key exists",
			init: [][]string{
				{"SET", "a", "1"},
			},
			args: args{cmdArgs: []string{"GET", "a"}},
			want: resp.BuildBulkString(strPtr("1")),
		},
		{
			name: "INCR key",
			init: [][]string{
				{"SET", "a", "1"},
				{"SET", "a", "6"},
				{"INCR", "a"},
			},
			args: args{cmdArgs: []string{"GET", "a"}},
			want: resp.BuildBulkString(strPtr("7")),
		},
		{
			name: "INCR key error",
			init: [][]string{
				{"SET", "a", "1"},
				{"SET", "a", "6c"},
			},
			args: args{cmdArgs: []string{"INCR", "a"}},
			want: resp.BuildErrorMsg(resp.ErrWrongType, "Wrong data type"),
		},
		{
			name: "EXISTS keys",
			init: [][]string{
				{"SET", "a", "1"},
				{"SET", "b", "6"},
			},
			args: args{cmdArgs: []string{"EXISTS", "a", "b", "c", "d"}},
			want: resp.BuildInteger(2),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core.ResetMemDB()
			for _, args := range tt.init {
				initCmd, _ := core.BuildCmd(args)
				core.ExecuteCmd(initCmd)
			}
			cmd, _ := core.BuildCmd(tt.args.cmdArgs)
			if got := core.ExecuteCmd(cmd); got != tt.want {
				t.Errorf("ExecuteCmd() = %v, want %v", got, tt.want)
			}
		})
	}
}
