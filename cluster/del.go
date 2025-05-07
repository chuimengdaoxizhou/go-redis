package cluster

import (
	"goredis/interface/resp"
	"goredis/resp/reply"
)

// Del 原子性地删除多个写键（writeKeys）
// 支持跨节点操作，如果键分布在不同的节点上，会广播指令并采用 try-commit-catch 模式删除
func Del(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 广播 DEL 命令给所有节点（每个节点会尝试删除它自己本地的数据）
	replies := cluster.broadcast(c, args)

	var errReply reply.ErrorReply // 用于记录第一个错误响应
	var deleted int64 = 0         // 累加删除成功的 key 数量

	for _, v := range replies {
		if reply.IsErrorReply(v) {
			// 如果有任何节点返回错误，记录下第一个错误
			errReply = v.(reply.ErrorReply)
			break // 一旦出现错误就停止统计
		}
		// 期望每个返回值都是整数类型（表示成功删除的键个数）
		intReply, ok := v.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error") // 类型不匹配也视为错误
		}
		// 累加各个节点返回的删除数量
		deleted += intReply.Code
	}

	// 如果没有任何错误，返回总共删除的键数
	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}

	// 否则，统一返回一个错误信息
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
