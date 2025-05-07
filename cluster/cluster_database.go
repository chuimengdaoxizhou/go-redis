// Package cluster 提供了一个对客户端透明的集群服务。
// 客户端可以连接集群中的任意节点，即可访问整个集群的数据。
package cluster

import (
	"context"
	"fmt"
	pool "github.com/jolestar/go-commons-pool/v2" // 对象池库，用于连接复用
	"goredis/config"                              // 配置项：包括节点自身地址与集群中的其他节点
	"goredis/database"                            // 单机版数据库实现
	databaseface "goredis/interface/database"     // 数据库接口
	"goredis/interface/resp"
	"goredis/lib/consistenthash" // 一致性哈希库，用于选择目标节点
	"goredis/lib/logger"
	"goredis/resp/reply"
	"runtime/debug"
	"strings"
)

// ClusterDatabase 表示集群中的一个 Redis 节点实例
// 每个节点持有部分数据，同时协调其他节点完成跨节点的事务
type ClusterDatabase struct {
	self string // 当前节点地址（host:port）

	nodes          []string                    // 集群中所有节点（包含自己）
	peerPicker     *consistenthash.NodeMap     // 一致性哈希节点选择器
	peerConnection map[string]*pool.ObjectPool // 各个 peer 节点的连接池
	db             databaseface.Database       // 当前节点本地数据库
}

// MakeClusterDatabase 初始化并启动一个集群节点
func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,            // 获取当前节点地址（从配置中读取）
		db:             database.NewStandaloneDatabase(),  // 初始化单机数据库作为本地存储
		peerPicker:     consistenthash.NewNodeMap(nil),    // 初始化一致性哈希选择器
		peerConnection: make(map[string]*pool.ObjectPool), // 创建连接池映射
	}

	// 收集所有节点地址（包括自身）
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self) // 加入自身节点
	cluster.peerPicker.AddNode(nodes...)          // 加入一致性哈希环
	cluster.nodes = nodes                         // 保存节点列表

	// 为每个 peer 节点创建一个连接池（不包括自己）
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer, // 每个节点创建自己的连接工厂
		})
	}

	return cluster
}

// CmdFunc 表示每个 Redis 命令对应的执行函数签名
type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply

// Close 关闭当前节点（释放本地数据库资源）
func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}

// router 是命令路由器，将命令名映射到具体执行函数
var router = makeRouter()

// Exec 执行客户端发送的命令（支持路由到本地或远程节点）
func (cluster *ClusterDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	// 捕获 panic，避免服务崩溃
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{} // 返回通用错误
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0])) // 获取命令名（转换为小写）
	cmdFunc, ok := router[cmdName]                 // 查找对应处理函数
	if !ok {
		// 如果命令不存在或在集群模式下不支持，返回错误
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdLine) // 调用具体命令处理函数
	return
}

// AfterClientClose 客户端断开连接后的清理操作（代理到本地数据库）
func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}
