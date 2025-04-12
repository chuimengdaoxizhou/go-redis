package database

import "goredis/interface/resp"

type CmdLine = [][]byte

type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply
	Close()
	AfterClientClose(c resp.Connection) // 关闭连接后执行的操作
}

type DataEntity struct {
	Data interface{}
}
