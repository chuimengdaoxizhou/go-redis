package database

import (
	"goredis/interface/resp"
	"goredis/lib/utils"
	"goredis/resp/reply"
)

// DEL
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	// 删除键
	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine2("del", args...))
	}
	return reply.MakeIntReply(int64(deleted))
}

// EXISTS
func execExists(db *DB, args [][]byte) resp.Reply {
	// 检查键是否存在
	exists := 0
	for _, arg := range args {
		key := string(arg)
		if _, ok := db.GetEntity(key); ok {
			exists++
		}
	}
	return reply.MakeIntReply(int64(exists))
}

//KEYS

// FLUSHDB
func exeFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine2("flushdb", args...))
	return reply.MakeOkReply()
}

// TYPE k1
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none") // none\r\n
	}
	switch entity.Data.(type) {
	case [][]byte:
		return reply.MakeStatusReply("string")
	}
	return reply.UnkoneErrReply{}
}

// RENAME input: k1 k2 如果 k2 存在，则覆盖
func execRename(db *DB, args [][]byte) resp.Reply {
	oldKey := string(args[0])
	newKey := string(args[1])
	entity, exists := db.GetEntity(oldKey)
	if !exists {
		return reply.MakeErrReply("no such key")
	}

	db.PutEntity(newKey, entity) // 将旧键的值赋值给新键
	db.Remove(oldKey)            // 删除旧键
	db.addAof(utils.ToCmdLine2("rename", args...))
	return reply.MakeOkReply()
}

// RENAMENX input: k1 k2 如果 k2 存在，则不执行操作
func execRenamenx(db *DB, args [][]byte) resp.Reply {
	oldKey := string(args[0])
	newKey := string(args[1])
	_, ok := db.GetEntity(newKey)
	if ok {
		return reply.MakeIntReply(0)
	}

	entity, exists := db.GetEntity(oldKey)
	if !exists {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(newKey, entity) // 将旧键的值赋值给新键
	db.Remove(oldKey)            // 删除旧键
	db.addAof(utils.ToCmdLine2("renamenx", args...))
	return reply.MakeIntReply(1)
}

func init() {
	RegisterCommand("DEL", execDel, -2)
	RegisterCommand("EXISTS", nil, -2)
	//RegisterCommand("KEYS", nil, -2)
	RegisterCommand("FLUSHDB", nil, -1)
	RegisterCommand("TYPE", nil, 2)
	RegisterCommand("RENAME", nil, 3)
	RegisterCommand("RENAMENX", nil, 3)
}
