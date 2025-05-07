package connection

import (
	"bytes"
	"goredis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection 表示与 redis-cli 的连接
type Connection struct {
	conn net.Conn // 网络连接对象
	// 等待直到回复完成
	waitingReply wait.Wait
	// 发送响应时的锁
	mu sync.Mutex
	// 选择的数据库索引
	selectedDB int
}

// NewConn 创建一个新的 Connection 实例
func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn, // 传入的 TCP 连接
	}
}

// RemoteAddr 返回远程网络地址
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr() // 返回连接的远程地址
}

// Close 关闭与客户端的连接
func (c *Connection) Close() error {
	// 等待直到回复完成，最多等待 10 秒
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	// 关闭 TCP 连接
	_ = c.conn.Close()
	return nil
}

// Write 通过 TCP 连接发送响应到客户端
func (c *Connection) Write(b []byte) error {
	// 如果没有数据要发送，直接返回
	if len(b) == 0 {
		return nil
	}
	// 获取锁，防止同时发送响应
	c.mu.Lock()
	// 增加等待计数，表示正在等待回复
	c.waitingReply.Add(1)
	// 在发送完后解锁
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	// 通过连接发送数据
	_, err := c.conn.Write(b)
	return err
}

// GetDBIndex 返回当前选择的数据库索引
func (c *Connection) GetDBIndex() int {
	return c.selectedDB // 返回 selectedDB 字段
}

// SelectDB 选择一个数据库
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum // 设置 selectedDB 字段为 dbNum
}

// FakeConn 实现了用于测试的 redis.Connection 接口
type FakeConn struct {
	Connection              // 嵌入 Connection 类型
	buf        bytes.Buffer // 用于模拟写入的缓冲区
}

// Write 将数据写入到缓冲区
func (c *FakeConn) Write(b []byte) error {
	c.buf.Write(b) // 将数据写入缓冲区
	return nil
}

// Clean 重置缓冲区
func (c *FakeConn) Clean() {
	c.buf.Reset() // 清空缓冲区
}

// Bytes 返回已写入缓冲区的数据
func (c *FakeConn) Bytes() []byte {
	return c.buf.Bytes() // 返回缓冲区中的字节数据
}
