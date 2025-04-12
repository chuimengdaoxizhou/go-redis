package parser

import (
	"bufio"
	"errors"
	log "github.com/sirupsen/logrus"
	"goredis/interface/resp"
	"goredis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Paylaod struct {
	Data resp.Reply
	Err  error
}

type readStata struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func (r *readStata) finished() bool {
	return r.expectedArgsCount > 0 && len(r.args) == r.expectedArgsCount
}

func Parsestream(reader io.Reader) <-chan *Paylaod {
	ch := make(chan *Paylaod)
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Paylaod) {
	defer func() {
		if err := recover(); err != nil {
			log.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readStata
	var err error
	var msg []byte
	for {
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr { // io错误直接退出
				ch <- &Paylaod{
					Err: err,
				}
				close(ch)
				return
			}
			// 不是io错误，继续解析
			ch <- &Paylaod{
				Err: err,
			}
			state = readStata{}
			continue
		}

		if state.readingMultiLine { // 判断是不是解析多行数据
			if msg[0] == '*' { // *3\r\n
				err := parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Paylaod{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readStata{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Paylaod{
						Data: reply.EmptyMultiBulkReply{},
					}
					state = readStata{}
					continue
				}
			} else if msg[0] == '$' { // $4\r\nPING\r\n
				err := parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Paylaod{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readStata{}
					continue
				}
				if state.bulkLen == -1 {
					ch <- &Paylaod{
						Data: reply.EmptyMultiBulkReply{},
					}
					state = readStata{}
					continue
				}
			} else {
				result, err := parseSingleLineReply(msg)
				ch <- &Paylaod{
					Data: result,
					Err:  err,
				}
				state = readStata{}
				continue
			}
		} else {
			err := readBody(msg, &state)
			if err != nil {
				ch <- &Paylaod{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readStata{}
				continue
			}
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Paylaod{
					Data: result,
					Err:  err,
				}
				state = readStata{}
			}
		}

	}
}

func readLine(bufReader *bufio.Reader, state *readStata) ([]byte, bool, error) {
	// 1.按照 \r\n 切分
	// 2.之前读到了$数字，严格读取字符个数
	var msg []byte
	var err error
	if state.bulkLen == 0 { // 1.按照 \r\n 切分
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // 2.之前读到了$数字，严格读取字符个数
		msg = make([]byte, state.bulkLen+2)
		_, err := io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

func parseMultiBulkHeader(msg []byte, stats *readStata) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		stats.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		stats.msgType = msg[0]
		stats.readingMultiLine = true
		stats.expectedArgsCount = int(expectedLine)
		stats.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseBulkHeader(msg []byte, state *readStata) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 {
		return nil
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

// 解析客户端发送给服务端 +OK -err 这种数据内容
// +OK\r\n -err\r\n :5\r\n
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

func readBody(msg []byte, state *readStata) error {
	line := msg[0 : len(msg)-2]
	var err error
	// $3
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		// $0\r\n
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
