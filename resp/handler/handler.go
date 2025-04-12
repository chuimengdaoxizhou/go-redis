package handler

import (
	"context"
	log "github.com/sirupsen/logrus"
	"goredis/database"
	databaseface "goredis/interface/database"
	"goredis/lib/sync/atomic"
	"goredis/resp/connection"
	"goredis/resp/parser"
	"goredis/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type RespHandler struct {
	activeConn sync.Map
	db         databaseface.Database
	closing    atomic.Boolean
}

func MakeHandler() *RespHandler {
	var db databaseface.Database
	db = database.NewDatabase()
	return &RespHandler{
		db: db,
	}
}

func (r *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	r.db.AfterClientClose(client)
	r.activeConn.Delete(client)
}
func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if r.closing.Get() {
		_ = conn.Close()
	}
	client := connection.NewConn(conn)
	r.activeConn.Store(client, struct{}{})
	ch := parser.Parsestream(conn)

	for payload := range ch {
		if payload.Err != nil { // 错误
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") { // 客户端关闭
				r.closeClient(client)
				log.Info("Connection closed" + client.RemoteAddr().String())
				return
			}
			// protocal 错误
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeClient(client)
				log.Info("Connection closed" + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil { // 空数据
			continue
		}
		reply, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			log.Info("require multi bulk reply")
			continue
		}

		result := r.db.Exec(client, reply.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			client.Write(unknownErrReplyBytes)
		}
	}
}

func (r *RespHandler) Close() error {
	log.Info("handler shutting down")
	r.closing.Set(true)
	r.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	r.db.Close()
	return nil
}
