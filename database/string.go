package database

import (
	"goredis/interface/database"
	"goredis/interface/resp"
	"goredis/lib/utils"
	"goredis/resp/reply"
	"strconv"
	"strings"
	"time"
)

// 获取指定键对应的字符串值
func (db *DB) getAsString(key string) ([]byte, reply.ErrorReply) {
	// 从数据库中获取键对应的实体
	entity, ok := db.GetEntity(key)
	if !ok { // 如果实体不存在，则返回 nil 和空错误
		return nil, nil
	}
	// 将实体的值转换为 []byte 类型
	bytes, ok := entity.Data.([]byte)
	if !ok { // 如果转换失败，返回错误
		return nil, &reply.WrongTypeErrReply{}
	}
	// 返回值和 nil 错误
	return bytes, nil
}

// 执行 GET 命令，获取键对应的值
func execGet(db *DB, args [][]byte) resp.Reply {
	// 获取键
	key := string(args[0])
	// 获取键对应的值
	bytes, err := db.getAsString(key)
	if err != nil { // 如果有错误，返回错误
		return err
	}
	if bytes == nil { // 如果值为空，返回空的 BulkReply
		return &reply.NullBulkReply{}
	}
	// 返回包含值的 BulkReply
	return reply.MakeBulkReply(bytes)
}

const (
	upsertPolicy = iota // 默认策略：更新或插入
	insertPolicy        // 插入策略：只有键不存在时插入
	updatePolicy        // 更新策略：只有键存在时更新
)

// 执行 SET 命令，设置键值
func execSet(db *DB, args [][]byte) resp.Reply {
	// 获取键和值
	key := string(args[0])
	value := args[1]
	// 默认策略：upsertPolicy
	policy := upsertPolicy
	var ttl int64 = 0 // 默认没有过期时间

	// 解析选项
	if len(args) > 2 {
		i := 2
		// 遍历选项参数
		for i < len(args) {
			arg := strings.ToUpper(string(args[i])) // 将参数转为大写
			// "NX" 选项：只有键不存在时才设置
			if arg == "NX" {
				if policy == updatePolicy { // 如果当前策略为更新策略，返回语法错误
					return &reply.SyntaxErrReply{}
				}
				policy = insertPolicy // 设置为插入策略
				i++
			} else if arg == "XX" { // "XX" 选项：只有键存在时才更新
				if policy == insertPolicy { // 如果当前策略为插入策略，返回语法错误
					return &reply.SyntaxErrReply{}
				}
				policy = updatePolicy // 设置为更新策略
				i++
			} else if arg == "EX" { // "EX" 选项：设置过期时间（单位：秒）
				if i+1 >= len(args) { // 如果没有后续参数，返回语法错误
					return &reply.SyntaxErrReply{}
				}
				seconds, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil || seconds <= 0 { // 如果转换失败或过期时间小于等于 0，返回错误
					return reply.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = seconds * 1000 // 将过期时间转换为毫秒
				i += 2
			} else { // 无效选项，返回语法错误
				return &reply.SyntaxErrReply{}
			}
		}
	}

	// 创建实体，并赋值给数据
	entity := &database.DataEntity{
		Data: value,
	}

	// 如果设置了过期时间，添加过期时间
	if ttl > 0 {
		entity.ExpireTime = time.Now().UnixNano()/1e6 + ttl // 当前时间加上过期时间
	}

	var result int
	// 根据策略进行相应的插入或更新操作
	switch policy {
	case upsertPolicy: // 默认：更新或插入
		db.PutEntity(key, entity)
		result = 1
	case insertPolicy: // 插入策略：键不存在时插入
		result = db.PutIfAbsent(key, entity)
	case updatePolicy: // 更新策略：键存在时更新
		result = db.PutIfExists(key, entity)
	}
	// 添加 AOF 命令记录
	db.addAof(utils.ToCmdLine2("set", args...))
	if result > 0 { // 如果操作成功，返回 OK 回复
		return &reply.OkReply{}
	}
	// 否则返回空的 BulkReply
	return &reply.NullBulkReply{}
}

// 执行 SETNX 命令：只有当键不存在时才设置值
func execSetNX(db *DB, args [][]byte) resp.Reply {
	// 获取键和值
	key := string(args[0])
	value := args[1]
	// 创建数据实体
	entity := &database.DataEntity{
		Data: value,
	}
	// 如果键不存在，插入数据并返回 1，否则返回 0
	result := db.PutIfAbsent(key, entity)
	// 添加 AOF 命令记录
	db.addAof(utils.ToCmdLine2("setnx", args...))
	// 返回插入结果：1 表示插入成功，0 表示键已存在
	return reply.MakeIntReply(int64(result))
}

// 执行 MSET 命令：批量设置多个键值对
func execMSet(db *DB, args [][]byte) resp.Reply {
	// 如果参数个数不是偶数，返回语法错误
	if len(args)%2 != 0 {
		return reply.MakeSyntaxErrReply()
	}

	// 计算键值对的数量
	size := len(args) / 2
	keys := make([]string, size)
	values := make([][]byte, size)

	// 将键值对拆分为键和值
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}

	// 将所有键值对插入数据库
	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}

	// 添加 AOF 命令记录
	db.addAof(utils.ToCmdLine2("mset", args...))
	// 返回 OK 回复
	return &reply.OkReply{}
}

// 执行 MGET 命令：批量获取多个键的值
func execMGet(db *DB, args [][]byte) resp.Reply {
	// 将参数转换为键的字符串切片
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	// 创建结果数组，用于存放每个键的值
	result := make([][]byte, len(args))

	// 遍历所有键，获取对应的值
	for i, key := range keys {
		bytes, err := db.getAsString(key)
		if err != nil { // 如果有错误
			// 如果错误类型是 WrongTypeErrReply，则说明键的类型不匹配，返回 nil
			_, isWrongType := err.(*reply.WrongTypeErrReply)
			if isWrongType {
				result[i] = nil
				continue
			} else {
				// 其他类型的错误直接返回
				return err
			}
		}
		// 将键的值存入结果数组（可能为 nil 或 []byte）
		result[i] = bytes
	}

	// 返回包含所有值的 MultiBulkReply
	return reply.MakeMultiBulkReply(result)
}

// 执行 MSETNX 命令：批量设置多个键值对，只有当所有键不存在时才设置
func execMSetNX(db *DB, args [][]byte) resp.Reply {
	// 解析参数，确保参数个数为偶数
	if len(args)%2 != 0 {
		return reply.MakeSyntaxErrReply()
	}

	// 计算键值对的数量
	size := len(args) / 2
	values := make([][]byte, size)
	keys := make([]string, size)

	// 将键值对拆分为键和值
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}

	// 检查所有键是否都不存在
	for _, key := range keys {
		_, exists := db.GetEntity(key)
		if exists { // 如果键已存在，返回 0
			return reply.MakeIntReply(0)
		}
	}

	// 所有键都不存在时，插入所有键值对
	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}

	// 添加 AOF 命令记录
	db.addAof(utils.ToCmdLine2("msetnx", args...))
	// 返回 1，表示成功插入所有键值对
	return reply.MakeIntReply(1)
}

// 执行 GETSET 命令：将键的值设置为指定的值，并返回原始值
func execGetSet(db *DB, args [][]byte) resp.Reply {
	// 获取键和值
	key := string(args[0])
	value := args[1]

	// 获取原始值
	old, err := db.getAsString(key)
	if err != nil {
		return err
	}

	// 将新值设置到数据库
	db.PutEntity(key, &database.DataEntity{Data: value})

	// 如果原始值为 nil，返回 NullBulkReply
	if old == nil {
		return new(reply.NullBulkReply)
	}

	// 添加 AOF 命令记录
	db.addAof(utils.ToCmdLine2("getset", args...))

	// 返回原始值
	return reply.MakeBulkReply(old)
}

// 执行 INCR 命令：将指定键的值增加 1
func execIncr(db *DB, args [][]byte) resp.Reply {
	// 获取键
	key := string(args[0])

	// 获取键的当前值
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}

	if bytes != nil {
		// 如果当前值为整数，进行自增操作
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			// 如果值不是整数，返回错误
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		// 更新键的值
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+1, 10)),
		})

		// 添加 AOF 命令记录
		db.addAof(utils.ToCmdLine2("incr", args...))

		// 返回更新后的值
		return reply.MakeIntReply(val + 1)
	}

	// 如果值为空（键不存在），则设置初始值为 1
	db.PutEntity(key, &database.DataEntity{
		Data: []byte("1"),
	})

	// 添加 AOF 命令记录
	db.addAof(utils.ToCmdLine2("incr", args...))

	// 返回 1，表示初始值
	return reply.MakeIntReply(1)
}

// 执行 INCRBY 命令：将指定键的值增加给定的增量
func execIncrBy(db *DB, args [][]byte) resp.Reply {
	// 获取键和增量值
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		// 如果增量不是整数，返回错误
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}

	// 获取键的当前值
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}

	if bytes != nil {
		// 如果当前值为整数，进行增量操作
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			// 如果值不是整数，返回错误
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		// 更新键的值
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+delta, 10)),
		})

		// 添加 AOF 命令记录
		db.addAof(utils.ToCmdLine2("incrby", args...))

		// 返回更新后的值
		return reply.MakeIntReply(val + delta)
	}

	// 如果值为空（键不存在），直接设置增量作为新值
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})

	// 添加 AOF 命令记录
	db.addAof(utils.ToCmdLine2("incrby", args...))

	// 返回增量值
	return reply.MakeIntReply(delta)
}

// 执行 DECR 命令：将指定键的值减少 1
func execDecr(db *DB, args [][]byte) resp.Reply {
	// 获取键
	key := string(args[0])

	// 获取键的当前值
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}

	if bytes != nil {
		// 如果当前值为整数，进行自减操作
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			// 如果值不是整数，返回错误
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		// 更新键的值
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-1, 10)),
		})

		// 添加 AOF 命令记录
		db.addAof(utils.ToCmdLine2("decr", args...))

		// 返回更新后的值
		return reply.MakeIntReply(val - 1)
	}

	// 如果值为空（键不存在），则设置初始值为 -1
	entity := &database.DataEntity{
		Data: []byte("-1"),
	}
	db.PutEntity(key, entity)

	// 添加 AOF 命令记录
	db.addAof(utils.ToCmdLine2("decr", args...))

	// 返回 -1，表示初始值
	return reply.MakeIntReply(-1)
}

// 执行 DECRBY 命令：将指定键的值减少给定的增量
func execDecrBy(db *DB, args [][]byte) resp.Reply {
	// 获取键和增量值
	key := string(args[0])
	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		// 如果增量不是整数，返回错误
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}

	// 获取键的当前值
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}

	if bytes != nil {
		// 如果当前值为整数，进行减量操作
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			// 如果值不是整数，返回错误
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		// 更新键的值
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-delta, 10)),
		})

		// 添加 AOF 命令记录
		db.addAof(utils.ToCmdLine2("decrby", args...))

		// 返回更新后的值
		return reply.MakeIntReply(val - delta)
	}

	// 如果值为空（键不存在），则设置初始值为 -delta
	valueStr := strconv.FormatInt(-delta, 10)
	db.PutEntity(key, &database.DataEntity{
		Data: []byte(valueStr),
	})

	// 添加 AOF 命令记录
	db.addAof(utils.ToCmdLine2("decrby", args...))

	// 返回 -delta，表示初始值
	return reply.MakeIntReply(-delta)
}

// execStrLen 返回指定 key 对应字符串值的长度
func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])            // 获取键名
	bytes, err := db.getAsString(key) // 从数据库获取字符串类型的值
	if err != nil {
		return err // 若获取失败，返回错误响应
	}
	if bytes == nil {
		return reply.MakeIntReply(0) // 若 key 不存在，返回 0
	}
	return reply.MakeIntReply(int64(len(bytes))) // 返回字符串长度
}

// execAppend 将给定值追加到 key 对应的字符串末尾，并返回新字符串的长度
func execAppend(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key) // 获取当前字符串值
	if err != nil {
		return err
	}
	bytes = append(bytes, args[1]...) // 将 args[1] 追加到字符串末尾
	db.PutEntity(key, &database.DataEntity{
		Data: bytes, // 更新数据库中的值
	})
	db.addAof(utils.ToCmdLine2("append", args...)) // 添加到 AOF 日志
	return reply.MakeIntReply(int64(len(bytes)))   // 返回新字符串长度
}

// execSetRange 从指定 offset 开始，用 value 内容覆盖字符串
func execSetRange(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	offset, errNative := strconv.ParseInt(string(args[1]), 10, 64) // 解析偏移量
	if errNative != nil {
		return reply.MakeErrReply(errNative.Error())
	}
	value := args[2]
	bytes, err := db.getAsString(key) // 获取原始字符串
	if err != nil {
		return err
	}
	bytesLen := int64(len(bytes))
	if bytesLen < offset {
		// 若 offset 超出原字符串长度，需补齐中间部分为 \x00
		diff := offset - bytesLen
		diffArray := make([]byte, diff) // 生成补零的字节数组
		bytes = append(bytes, diffArray...)
		bytesLen = int64(len(bytes))
	}
	// 用 value 中的数据覆盖原字符串从 offset 开始的位置
	for i := 0; i < len(value); i++ {
		idx := offset + int64(i)
		if idx >= bytesLen {
			bytes = append(bytes, value[i]) // 若超出末尾则追加
		} else {
			bytes[idx] = value[i] // 否则直接替换
		}
	}
	db.PutEntity(key, &database.DataEntity{
		Data: bytes,
	})
	db.addAof(utils.ToCmdLine2("setRange", args...)) // 添加 AOF 日志
	return reply.MakeIntReply(int64(len(bytes)))     // 返回新字符串长度
}

// execGetRange 获取字符串中指定范围的子串（支持负数索引）
func execGetRange(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	startIdx, errNative := strconv.ParseInt(string(args[1]), 10, 64) // 起始索引
	if errNative != nil {
		return reply.MakeErrReply(errNative.Error())
	}
	endIdx, errNative := strconv.ParseInt(string(args[2]), 10, 64) // 结束索引
	if errNative != nil {
		return reply.MakeErrReply(errNative.Error())
	}

	bytes, err := db.getAsString(key) // 获取字符串
	if err != nil {
		return err
	}

	if bytes == nil {
		return reply.MakeNullBulkReply() // key 不存在，返回 Null
	}

	bytesLen := int64(len(bytes)) // 字符串长度

	// 处理 start 索引越界或负值
	if startIdx < -1*bytesLen {
		return &reply.NullBulkReply{}
	} else if startIdx < 0 {
		startIdx = bytesLen + startIdx // 转换为正向索引
	} else if startIdx >= bytesLen {
		return &reply.NullBulkReply{}
	}

	// 处理 end 索引越界或负值
	if endIdx < -1*bytesLen {
		return &reply.NullBulkReply{}
	} else if endIdx < 0 {
		endIdx = bytesLen + endIdx + 1 // 注意 end 是闭区间，所以需要 +1
	} else if endIdx < bytesLen {
		endIdx = endIdx + 1 // 包含 end 本身
	} else {
		endIdx = bytesLen // 截止到最后
	}

	if startIdx > endIdx {
		return reply.MakeNullBulkReply() // 若区间非法，返回空
	}

	return reply.MakeBulkReply(bytes[startIdx:endIdx]) // 返回子串
}

func init() {
	RegisterCommand("Set", execSet, -3)
	RegisterCommand("SetNx", execSetNX, 3)
	RegisterCommand("MSet", execMSet, -3)
	RegisterCommand("MGet", execMGet, -2)
	RegisterCommand("MSetNX", execMSetNX, -3)
	RegisterCommand("Get", execGet, 2)
	RegisterCommand("GetSet", execGetSet, 3)
	RegisterCommand("Incr", execIncr, 2)
	RegisterCommand("IncrBy", execIncrBy, 3)
	RegisterCommand("Decr", execDecr, 2)
	RegisterCommand("DecrBy", execDecrBy, 3)
	RegisterCommand("StrLen", execStrLen, 2)
	RegisterCommand("Append", execAppend, 3)
	RegisterCommand("SetRange", execSetRange, 4)
	RegisterCommand("GetRange", execGetRange, 4)
}
