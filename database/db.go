package database

import (
	"goredis/datastruct/dict"
	"goredis/interface/database"
	"goredis/interface/resp"
	"goredis/resp/reply"
	"strings"
)

type DB struct {
	index  int
	data   dict.Dict
	addAof func(CmdLine)
}

type ExecFunc func(db *DB, args [][]byte) resp.Reply // 执行函数

type CmdLine = [][]byte

func MakeDB() *DB {
	return &DB{
		data:   dict.MakeSyncDict(),
		addAof: func(line CmdLine) {},
	}
}

// 执行用户发送的命令
func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	name := strings.ToLower(string(cmdLine[0])) // 命令转换为小写
	cmd, ok := cmdTable[name]
	if !ok {
		return reply.MakeErrReply("ERR unknown command " + name)
	}
	if !availableArgsCount(cmd.arity, cmdLine[1:]) {
		return reply.MakeArgNumErrReply(name)
	}
	fun := cmd.exector

	return fun(db, cmdLine[1:]) // 执行命令 只需要获取参数，不需要获取命令
}

// availableArgsCount 检查参数个数是否正确
// SET K V -》 3 对于确定参数个数的命令，arity = 3
// EXISTS K1 K2 ... -》 -2 对于不确定参数个数的命令，arity = -2
func availableArgsCount(arity int, args [][]byte) bool {
	argNum := len(args)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if !exists {
			db.Removes(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush() {
	db.data.Clear()
}
