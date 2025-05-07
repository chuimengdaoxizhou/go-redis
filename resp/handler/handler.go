package handler

/*
 * RespHandler 实现了 Redis 协议
 */

import (
	"context"
	"goredis/cluster"
	"goredis/config"
	"goredis/database"
	databaseface "goredis/interface/database"
	"goredis/lib/logger"
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
	// unknownErrReplyBytes 用于表示未知错误的回复
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// RespHandler 实现了 tcp.Handler 接口，充当 Redis 请求的处理器
type RespHandler struct {
	activeConn sync.Map              // 存储活动连接的映射，key 为 *client，value 为占位符
	db         databaseface.Database // 数据库实例
	closing    atomic.Boolean        // 用于标记是否正在关闭，拒绝新连接和请求
}

// MakeHandler 创建并返回一个 RespHandler 实例
func MakeHandler() *RespHandler {
	var db databaseface.Database
	// 如果配置中包含集群信息，则使用集群数据库
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		db = cluster.MakeClusterDatabase()
	} else {
		// 否则，使用独立数据库
		db = database.NewStandaloneDatabase()
	}

	return &RespHandler{
		db: db,
	}
}

// closeClient 关闭客户端连接并清理资源
func (h *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()            // 关闭连接
	h.db.AfterClientClose(client) // 数据库执行客户端关闭后的清理操作
	h.activeConn.Delete(client)   // 从活动连接映射中删除客户端
}

// Handle 处理 TCP 连接，接收请求并执行相应操作
func (h *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// 如果 handler 正在关闭，拒绝新连接
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn) // 创建一个新的连接实例
	h.activeConn.Store(client, 1)      // 将客户端加入到活动连接映射中

	// 使用 parser 解析请求流
	ch := parser.ParseStream(conn)
	for payload := range ch {
		// 处理解析后的请求数据
		if payload.Err != nil {
			// 如果发生错误，检查是否是连接关闭相关的错误
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 如果连接已关闭，进行清理
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// 如果是协议错误，返回错误回复
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes()) // 写入错误回复
			if err != nil {
				h.closeClient(client) // 写入失败，关闭连接
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}

		// 处理有效的请求数据
		if payload.Data == nil {
			// 如果负载数据为空，记录错误并跳过
			logger.Error("empty payload")
			continue
		}

		// 将负载数据转换为 MultiBulkReply 类型
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			// 如果无法转换为 MultiBulkReply 类型，记录错误并跳过
			logger.Error("require multi bulk reply")
			continue
		}

		// 执行数据库操作
		result := h.db.Exec(client, r.Args)
		if result != nil {
			// 如果有结果，写入到客户端
			_ = client.Write(result.ToBytes())
		} else {
			// 如果没有结果，返回未知错误回复
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close 停止处理器并关闭所有活动连接
func (h *RespHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true) // 设置关闭标志
	// TODO: 等待并发操作完成
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close() // 关闭每个活动连接
		return true
	})
	h.db.Close() // 关闭数据库
	return nil
}
