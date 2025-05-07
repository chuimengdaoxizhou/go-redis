package cluster

import (
	"context"
	"errors"
	"github.com/jolestar/go-commons-pool/v2" // 对象池库
	"goredis/resp/client"                    // 自定义的 Redis 客户端包
)

// connectionFactory 是一个连接工厂，用于在连接池中创建、验证、销毁 Redis 节点连接。
type connectionFactory struct {
	Peer string // Redis 节点的地址，例如 "127.0.0.1:6379"
}

// MakeObject 创建一个新的 Redis 客户端连接，并将其包装为 PooledObject。
func (f *connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	// 创建一个新的 Redis 客户端，连接目标节点
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err // 创建失败，返回错误
	}
	c.Start()                           // 启动客户端（可能建立底层连接、监听响应等）
	return pool.NewPooledObject(c), nil // 包装成 PooledObject 并返回
}

// DestroyObject 用于销毁连接对象，当连接被从池中移除时调用
func (f *connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	// 类型断言，将池对象还原为 Redis 客户端
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch") // 类型错误
	}
	c.Close() // 正常关闭连接资源
	return nil
}

// ValidateObject 验证对象是否有效（连接是否健康），返回 true 表示可用
func (f *connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	// 此处可以实现 ping、状态检查等逻辑，目前直接返回 true（总是有效）
	return true
}

// ActivateObject 激活连接对象，表示即将开始使用（可做清理或准备）
func (f *connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	// 此处可添加重置状态等逻辑，目前为空实现
	return nil
}

// PassivateObject 钝化连接对象，表示当前使用完毕准备归还池中（可做资源释放等）
func (f *connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	// 通常用于清理上下文状态或缓冲区，此处为空实现
	return nil
}
