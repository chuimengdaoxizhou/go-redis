package reply

import (
	"bytes"
	"goredis/interface/resp"
	"strconv"
)

var(
	nullBulkReplyBytes = []byte("$-1")
	CRLF = "\r\n"
)

type BullkRely struct {
	Arg []byte
}

func (b BullkRely) ToBytes() []byte {     // redis 转换为 $7\r\nredis\r\n
	if len(b.Arg) == 0 {
		return nullBulkReplyBytes
	}
	return []byte("$" + strconv.Itoa(len(b.Arg)) + CRLF + string(b.Arg) + CRLF)
}

func MakeBulkReply(arg []byte) *BullkRely {
	return &BullkRely{Arg: arg}
}

type MultiBulkReply struct {
	Args [][]byte
}

func (m *MultiBulkReply) ToBytes() []byte {
	argLen := len(m.Args)
	var buf bytes.Buffer
	buf.WriteString(("*" + strconv.Itoa(argLen) + CRLF))

	for _, arg := range m.Args {
		if arg == nil {
			buf.WriteString(string(nullBulkReplyBytes) + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}
func MakeMultiBulkReply(arg [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args:arg}
}

type StstusReply struct {
	Ststus string
}

func MakeStatusReply(status string) *StstusReply {
	return &StstusReply{Ststus: status}
}

func (r *StstusReply) ToBytes() []byte {
	return []byte("+" + r.Ststus + CRLF)
}

type IntReply struct {
	Code int64
}
func MakeIntReply(code int64) *IntReply {
	return &IntReply{Code:code}
}

func (r *IntReply) ToBytes() []byte{
	return []byte(":" + strconv.FormatInt(r.Code,10) + CRLF)
}

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}


// 异常回复
type StandardErrReply struct {
	Status string
}

func (s *StandardErrReply) ToBytes() []byte {
	return []byte("-" + s.Status + CRLF)
}
func (s *StandardErrReply) Error() string {
	return s.Status
}
func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{Status: status}
}


func IsErrReply(reply resp.Reply) bool {
	return reply.ToBytes()[0] == '-'
}