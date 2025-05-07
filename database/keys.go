package database

import (
	"goredis/datastruct/sortedset"
	"goredis/interface/resp"
	"goredis/lib/utils"
	"goredis/lib/wildcard"
	"goredis/resp/reply"
	"strconv"
	"time"
)

// execDel 移除数据库中的一个或多个键
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	// 删除指定的键并返回删除的数量
	deleted := db.Removes(keys...)
	if deleted > 0 {
		// 如果删除了键，记录 AOF 操作日志
		db.addAof(utils.ToCmdLine2("del", args...))
	}
	// 返回删除的键数量
	return reply.MakeIntReply(int64(deleted))
}

// execExists 检查数据库中是否存在指定的键
func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	// 遍历所有提供的键
	for _, arg := range args {
		key := string(arg)
		// 获取实体并判断是否存在
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	// 返回存在的键的数量
	return reply.MakeIntReply(result)
}

// execFlushDB 删除当前数据库中的所有数据
func execFlushDB(db *DB, args [][]byte) resp.Reply {
	// 清空数据库
	db.Flush()
	// 记录 AOF 操作日志
	db.addAof(utils.ToCmdLine2("flushdb", args...))
	return &reply.OkReply{}
}

// execType 返回指定键的类型，包括：string、list、hash、set 和 zset
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	// 获取实体并检查其类型
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none") // 键不存在时返回 none
	}
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string") // 字符串类型
	case *sortedset.SortedSet:
		return reply.MakeStatusReply("zset") // 排序集合类型
	}
	// 对未知类型返回错误
	return &reply.UnknownErrReply{}
}

// execRename 重命名数据库中的一个键
func execRename(db *DB, args [][]byte) resp.Reply {
	// 检查参数数量
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])  // 源键
	dest := string(args[1]) // 目标键

	// 获取源键的实体
	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key") // 如果源键不存在，返回错误
	}
	// 将源键的实体存入目标键
	db.PutEntity(dest, entity)
	// 删除源键
	db.Remove(src)
	// 记录 AOF 操作日志
	db.addAof(utils.ToCmdLine2("rename", args...))
	return &reply.OkReply{}
}

// execRenameNx 重命名数据库中的一个键，仅当目标键不存在时
func execRenameNx(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])  // 源键
	dest := string(args[1]) // 目标键

	// 检查目标键是否存在
	_, ok := db.GetEntity(dest)
	if ok {
		return reply.MakeIntReply(0) // 如果目标键已存在，返回 0
	}

	// 获取源键的实体
	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key") // 如果源键不存在，返回错误
	}
	// 删除源键和目标键（清除 TTL）
	db.Removes(src, dest)
	// 将源键的实体存入目标键
	db.PutEntity(dest, entity)
	// 记录 AOF 操作日志
	db.addAof(utils.ToCmdLine2("renamenx", args...))
	return reply.MakeIntReply(1) // 返回 1，表示成功
}

// execKeys 返回所有符合给定模式的键
func execKeys(db *DB, args [][]byte) resp.Reply {
	// 编译给定的通配符模式
	pattern := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)
	// 遍历所有数据库中的键
	db.data.ForEach(func(key string, val interface{}) bool {
		// 如果键符合模式，则将其添加到结果中
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	// 返回符合条件的键列表
	return reply.MakeMultiBulkReply(result)
}

// execExpire 设置指定键的过期时间
func execExpire(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])

	// 解析过期时间（秒）
	seconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range") // 如果解析失败，返回错误
	}
	if seconds <= 0 {
		// 如果过期时间为非正数，则删除该键
		db.Remove(key)
		return reply.MakeIntReply(1)
	}

	// 获取实体
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(0) // 键不存在，返回 0
	}

	// 计算过期时间（毫秒）
	expireTime := time.Now().UnixNano()/1e6 + seconds*1000 // 转换为毫秒
	entity.ExpireTime = expireTime

	// 将更新后的实体存入数据库
	db.PutEntity(key, entity)

	// 记录 AOF 操作日志
	db.addAof(utils.ToCmdLine2("expire", args...))
	return reply.MakeIntReply(1) // 返回 1，表示设置成功
}

// execTTL 返回指定键的剩余过期时间
func execTTL(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])

	// 获取实体
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(-2) // 键不存在，返回 -2
	}

	// 如果没有过期时间
	if entity.ExpireTime == 0 {
		return reply.MakeIntReply(-1) // 返回 -1，表示没有过期时间
	}

	// 计算剩余过期时间
	now := time.Now().UnixNano() / 1e6 // 当前时间（毫秒）
	remaining := entity.ExpireTime - now

	if remaining <= 0 {
		// 键已过期
		db.Remove(key)
		return reply.MakeIntReply(-2) // 返回 -2，表示已过期
	}

	// 返回剩余过期时间（秒）
	return reply.MakeIntReply(remaining / 1000) // 转换为秒
}

func init() {
	// 注册各个命令及其对应的执行函数
	RegisterCommand("Del", execDel, -2)
	RegisterCommand("Exists", execExists, -2)
	RegisterCommand("Keys", execKeys, 2)
	RegisterCommand("FlushDB", execFlushDB, -1)
	RegisterCommand("Type", execType, 2)
	RegisterCommand("Rename", execRename, 3)
	RegisterCommand("RenameNx", execRenameNx, 3)
	RegisterCommand("Expire", execExpire, 3)
	RegisterCommand("TTL", execTTL, 2)
}
