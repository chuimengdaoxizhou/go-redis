package cluster

import (
	"goredis/interface/resp"
	"goredis/resp/reply"
)

// FlushDB 清空当前数据库中的所有数据（对整个集群生效）
// 将命令广播到集群中所有节点，逐个清空各自节点上的数据
func FlushDB(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 向所有节点广播 FLUSHDB 命令，让每个节点都清空自己本地的数据库
	replies := cluster.broadcast(c, args)

	var errReply reply.ErrorReply // 记录第一个出现的错误回复

	for _, v := range replies {
		if reply.IsErrorReply(v) {
			// 如果有任意一个节点返回错误，记录下并跳出循环
			errReply = v.(reply.ErrorReply)
			break
		}
	}

	// 如果所有节点均成功清空数据，则返回 OK 响应
	if errReply == nil {
		return &reply.OkReply{}
	}

	// 否则返回统一的错误信息，包含错误详情
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
