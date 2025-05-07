package cluster

import (
	"goredis/interface/resp"
	"goredis/resp/reply"
)

// Rename 重命名一个键（key）
// 要求源键（origin）和目标键（destination）必须位于同一个节点（即同一个哈希槽）
// 如果两个键不在同一节点上，则不允许重命名，因为 Redis 集群模式不支持跨槽的 rename 操作
func Rename(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 参数数量校验，rename 命令要求三个参数：RENAME 源键 目标键
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}

	// 提取源键和目标键的字符串表示
	src := string(args[1])
	dest := string(args[2])

	// 通过一致性哈希算法找出源键所在的节点
	srcPeer := cluster.peerPicker.PickNode(src)
	// 同样找出目标键所在的节点
	destPeer := cluster.peerPicker.PickNode(dest)

	// 如果两个键不在同一节点（即不同哈希槽），则不允许执行重命名操作
	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	}

	// 如果两个键在同一节点，则将 rename 命令转发到目标节点执行
	return cluster.relay(srcPeer, c, args)
}
