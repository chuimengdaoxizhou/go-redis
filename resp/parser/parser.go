package parser

import (
	"bufio"
	"errors"
	"goredis/interface/resp"
	"goredis/lib/logger"
	"goredis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// Payload 用于封装解析结果，包含解析的回复数据和错误信息
type Payload struct {
	Data resp.Reply
	Err  error
}

// ParseStream 从 io.Reader 中读取数据并解析，返回一个包含解析结果的 channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parseScore(reader, ch) // 使用协程解析数据
	return ch
}

// readState 记录解析的状态信息
type readState struct {
	readingMultiLine  bool     // 是否正在解析多行数据
	expectedArgsCount int      // 当前预期的参数个数
	msgType           byte     // 消息类型
	args              [][]byte // 消息参数
	bulkLen           int64    // bulk 数据的长度
}

// finished 判断解析是否完成
func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// parseScore 解析从客户端传来的请求
func parseScore(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack())) // 出现异常时记录堆栈
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		var ioErr bool
		// 读取一行数据
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			// 如果是 IO 错误，则发送错误并关闭通道
			if ioErr {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// 如果是协议错误，则重新读取状态
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// 解析读取的行
		if !state.readingMultiLine {
			// 如果是多行消息，则解析多行
			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // 重新设置状态
					continue
				}
				// 如果预期参数个数为0，返回空的多条消息
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' { // 如果是单条回复，解析单条消息头
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}
				// 如果 bulkLen 为-1，说明为空的回复
				if state.bulkLen == -1 {
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{}
					continue
				}
			} else {
				// 如果是单行回复，直接解析
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			// 如果正在解析多行消息体，则读取消息体
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{}
				continue
			}
			// 如果所有参数都已经读取完成
			if state.finished() {
				var result resp.Reply
				// 如果是多条消息
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					// 如果是单条消息
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// readLine 从 bufio.Reader 中读取一行数据
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	// 如果当前没有读取剩余的 bulk 数据，则正常读取一行
	if state.bulkLen == 0 {
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		// 如果行结尾没有 \r\n，则是协议错误
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		// 如果正在读取 bulk 数据，则按指定长度读取
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		// 检查行结尾是否符合协议
		if len(msg) == 0 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

// parseMultiBulkHeader 解析多条消息头
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	// 解析出预期的参数个数
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// parseBulkHeader 解析单条消息头
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 {
		return nil // 如果是空的 bulk 数据，则直接返回
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// parseSingleLineReply 解析单行回复
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+': // 状态恢复
		result = reply.MakeStatusReply(str[1:])
	case '-': // 错误回复
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	default:
		// 解析文本协议
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = reply.MakeMultiBulkReply(args)
	}
	return result, nil
}

// readBody 解析消息体
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
