package database

import (
	"goredis/interface/resp"
	"goredis/resp/reply"
)

// Ping 服务端
func Ping(db *DB, args [][]byte) resp.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply(string(args[0]))
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

func init() {
	RegisterCommand("ping", Ping, -1)
}
