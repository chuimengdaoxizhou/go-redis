package database

import "strings"

var cmdTable = make(map[string]*command) // 命令表

type command struct {
	exector ExecFunc
	arity   int
}

func RegisterCommand(name string, exector ExecFunc, arity int) {
	name = strings.ToLower(name) // 命令转换为小写
	cmdTable[name] = &command{
		exector: exector,
		arity:   arity,
	}
}
