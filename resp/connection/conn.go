package connection

import (
	"goredis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// 协议层对每一个client的描述
type Connection struct {
	conn         net.Conn
	waitingReply wait.Wait
	mu           sync.Mutex
	selectedDB   int
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Write(b []byte) (err error) {
	if len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()
	_, err = c.conn.Write(b)
	return err
}

func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeOut(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}
func (c *Connection) SelectDB(dbIndex int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selectedDB = dbIndex
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,

	}
}
