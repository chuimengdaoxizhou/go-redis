package cluster

import "goredis/interface/resp"

// CmdLine 是 [][]byte 的别名，表示一条命令及其参数（例如：["set", "key", "value"]）
type CmdLine = [][]byte

// makeRouter 构造 Redis 集群模式下的命令路由器
// 将命令名称映射到对应的处理函数（CmdFunc）
// 在执行命令时通过查表找到对应的处理逻辑
func makeRouter() map[string]CmdFunc {
	// 创建命令路由表
	routerMap := make(map[string]CmdFunc)

	// ping 命令，检查连接是否正常
	routerMap["ping"] = ping

	// 删除命令，支持跨节点删除多个键
	routerMap["del"] = Del

	// 以下命令默认只作用于一个 key，因此可以使用 defaultFunc 统一处理
	routerMap["exists"] = defaultFunc // 检查 key 是否存在
	routerMap["type"] = defaultFunc   // 返回 key 的数据类型
	routerMap["rename"] = Rename      // 重命名 key，要求两个 key 在同一节点
	routerMap["renamenx"] = Rename    // 同上，但只在目标 key 不存在时才执行

	routerMap["set"] = defaultFunc    // 设置 key 的值
	routerMap["setnx"] = defaultFunc  // 仅在 key 不存在时设置
	routerMap["get"] = defaultFunc    // 获取 key 的值
	routerMap["getset"] = defaultFunc // 设置新值并返回旧值

	// 清空当前数据库中的所有 key，会广播给所有节点
	routerMap["flushdb"] = FlushDB

	return routerMap
}

// defaultFunc 是默认的命令处理函数，适用于只操作一个 key 的命令
// 它会将命令转发到 key 所在的节点（由一致性哈希确定），并返回该节点的响应
func defaultFunc(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 提取 key（通常位于参数 args[1]，例如 set key value）
	key := string(args[1])

	// 通过一致性哈希找到负责该 key 的节点
	peer := cluster.peerPicker.PickNode(key)

	// 将命令转发给目标节点，并获取结果
	return cluster.relay(peer, c, args)
}
