package database

import (
	"goredis/interface/resp"
	"goredis/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}

func init() {
	RegisterCommand("PING", Ping, 1)
}
