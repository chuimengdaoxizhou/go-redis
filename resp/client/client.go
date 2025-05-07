package client

import (
	"goredis/interface/resp"
	"goredis/lib/logger"
	"goredis/lib/sync/wait"
	"goredis/resp/parser"
	"goredis/resp/reply"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

// Client 是一个使用管道模式的 Redis 客户端
type Client struct {
	conn        net.Conn        // TCP 连接
	pendingReqs chan *request   // 等待发送的请求队列
	waitingReqs chan *request   // 等待响应的请求队列
	ticker      *time.Ticker    // 心跳定时器
	addr        string          // Redis 服务地址
	working     *sync.WaitGroup // 用于跟踪正在进行的请求数（包括等待和待发送的请求）
}

// request 表示发送给 Redis 服务器的请求
type request struct {
	id        uint64     // 请求的唯一标识
	args      [][]byte   // 请求参数
	reply     resp.Reply // 响应
	heartbeat bool       // 是否为心跳请求
	waiting   *wait.Wait // 等待响应的机制
	err       error      // 错误信息
}

const (
	chanSize = 256             // 通道缓冲区大小
	maxWait  = 3 * time.Second // 最大等待时间
)

// MakeClient 创建一个新的客户端实例
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr) // 连接到指定的 Redis 服务
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

// Start 启动客户端的异步 goroutine
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second) // 设置心跳定时器
	go client.handleWrite()                          // 启动处理发送请求的 goroutine
	go func() {
		err := client.handleRead() // 启动处理接收响应的 goroutine
		if err != nil {
			logger.Error(err) // 处理错误
		}
	}()
	go client.heartbeat() // 启动心跳检查
}

// Close 停止异步 goroutine 并关闭连接
func (client *Client) Close() {
	client.ticker.Stop()      // 停止定时器
	close(client.pendingReqs) // 关闭待发送请求的通道

	// 等待所有请求处理完成
	client.working.Wait()

	// 关闭连接并清理
	_ = client.conn.Close()
	close(client.waitingReqs)
}

// handleConnectionError 处理连接错误并尝试重新连接
func (client *Client) handleConnectionError(err error) error {
	err1 := client.conn.Close() // 关闭旧连接
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" { // 如果错误不是连接已关闭，则返回错误
				return err1
			}
		} else {
			return err1
		}
	}
	// 尝试重新连接
	conn, err1 := net.Dial("tcp", client.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}
	client.conn = conn
	go func() {
		_ = client.handleRead() // 重新启动读取响应的 goroutine
	}()
	return nil
}

// heartbeat 定时发送心跳请求
func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat() // 执行心跳请求
	}
}

// handleWrite 处理发送请求
func (client *Client) handleWrite() {
	for req := range client.pendingReqs { // 从待发送请求队列中读取请求
		client.doRequest(req) // 发送请求
	}
}

// Send 发送请求到 Redis 服务器
func (client *Client) Send(args [][]byte) resp.Reply {
	request := &request{
		args:      args,
		heartbeat: false, // 标记这是一个普通请求
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)                              // 等待响应
	client.working.Add(1)                               // 增加工作计数
	defer client.working.Done()                         // 请求完成后减少计数
	client.pendingReqs <- request                       // 将请求加入待发送队列
	timeout := request.waiting.WaitWithTimeout(maxWait) // 等待最大超时
	if timeout {
		return reply.MakeErrReply("server time out") // 超时返回错误
	}
	if request.err != nil {
		return reply.MakeErrReply("request failed") // 请求失败返回错误
	}
	return request.reply // 返回响应
}

// doHeartbeat 执行心跳请求
func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")}, // 心跳请求参数
		heartbeat: true,                     // 标记这是一个心跳请求
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)                   // 等待响应
	client.working.Add(1)                    // 增加工作计数
	defer client.working.Done()              // 请求完成后减少计数
	client.pendingReqs <- request            // 将心跳请求加入待发送队列
	request.waiting.WaitWithTimeout(maxWait) // 等待响应
}

// doRequest 发送请求并处理可能的错误
func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	re := reply.MakeMultiBulkReply(req.args) // 创建多条命令的回复
	bytes := re.ToBytes()                    // 转换为字节数组
	_, err := client.conn.Write(bytes)       // 发送请求
	i := 0
	// 如果连接出现错误，尝试重新连接并重发请求，最多尝试 3 次
	for err != nil && i < 3 {
		err = client.handleConnectionError(err)
		if err == nil {
			_, err = client.conn.Write(bytes)
		}
		i++
	}
	if err == nil {
		client.waitingReqs <- req // 请求成功，加入等待响应队列
	} else {
		req.err = err      // 记录错误
		req.waiting.Done() // 完成等待
	}
}

// finishRequest 处理请求的响应
func (client *Client) finishRequest(reply resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack() // 打印堆栈信息
			logger.Error(err)  // 记录错误
		}
	}()
	request := <-client.waitingReqs // 从等待响应队列中获取请求
	if request == nil {
		return
	}
	request.reply = reply // 设置请求的响应
	if request.waiting != nil {
		request.waiting.Done() // 完成等待
	}
}

// handleRead 处理读取响应
func (client *Client) handleRead() error {
	ch := parser.ParseStream(client.conn) // 解析响应数据流
	for payload := range ch {             // 遍历响应数据
		if payload.Err != nil {
			client.finishRequest(reply.MakeErrReply(payload.Err.Error())) // 处理错误响应
			continue
		}
		client.finishRequest(payload.Data) // 处理正常响应
	}
	return nil
}
