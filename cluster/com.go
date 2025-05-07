package cluster

import (
	"context"
	"errors"
	"goredis/interface/resp"
	"goredis/lib/utils"
	"goredis/resp/client"
	"goredis/resp/reply"
	"strconv"
)

// getPeerClient 从连接池中获取一个连接到目标 peer 节点的 client 实例
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found") // 找不到目标节点的连接池
	}
	raw, err := factory.BorrowObject(context.Background()) // 从连接池借出一个连接对象
	if err != nil {
		return nil, err // 借连接失败
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type") // 类型转换失败
	}
	return conn, nil
}

// returnPeerClient 将连接归还给指定 peer 节点的连接池
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found") // 找不到对应的连接池
	}
	return connectionFactory.ReturnObject(context.Background(), peerClient) // 归还连接
}

// relay 用于将命令转发给指定的 peer 节点
// 注意：
//   - 如果目标是本节点，则直接调用本地数据库执行命令
//   - 否则通过网络连接发送命令
//   - 自动为连接选择对应的 DB（SELECT index）
//   - 不允许调用自身的事务命令（Prepare, Commit, Rollback）
func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self {
		// 如果目标节点是自己，直接调用本地数据库执行命令
		return cluster.db.Exec(c, args)
	}

	// 否则获取远程连接
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error()) // 获取连接失败，返回错误
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient) // 使用完毕后归还连接
	}()

	// 设置使用相同的 DB（保持一致性）
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))

	// 转发真实命令
	return peerClient.Send(args)
}

// broadcast 向集群中的所有节点广播命令（包括自身）
// 用于执行需要全局一致的命令，例如 FLUSHALL
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	result := make(map[string]resp.Reply) // 每个节点对应一个执行结果
	for _, node := range cluster.nodes {
		reply := cluster.relay(node, c, args) // 对每个节点进行转发
		result[node] = reply
	}
	return result
}
