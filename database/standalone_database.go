package database

import (
	"fmt"
	"goredis/aof"
	"goredis/config"
	"goredis/interface/resp"
	"goredis/lib/logger"
	"goredis/resp/reply"
	"runtime/debug"
	"strconv"
	"strings"
)

// StandaloneDatabase 表示单机版 Redis 数据库
// 该结构体管理多个数据库实例，并处理 AOF 持久化
type StandaloneDatabase struct {
	dbSet      []*DB           // 数据库集合，存储多个数据库实例
	aofHandler *aof.AofHandler // AOF 持久化处理器
}

// NewStandaloneDatabase 创建一个新的 StandaloneDatabase 实例
func NewStandaloneDatabase() *StandaloneDatabase {
	// 初始化 StandaloneDatabase 实例
	mdb := &StandaloneDatabase{}
	// 如果配置文件中的数据库数量为 0，设置默认值为 16
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	// 根据配置的数据库数量，创建多个数据库实例
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()    // 创建单个数据库实例
		singleDB.index = i      // 设置数据库的索引
		mdb.dbSet[i] = singleDB // 将数据库实例添加到数据库集合中
	}
	// 如果配置了 AOF 持久化，初始化 AOF 处理器
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(mdb) // 创建 AOF 处理器
		if err != nil {
			panic(err) // 如果 AOF 处理器创建失败，触发 panic
		}
		mdb.aofHandler = aofHandler
		// 为每个数据库实例设置 AOF 写入方法
		for _, db := range mdb.dbSet {
			// 避免闭包捕获
			singleDB := db
			singleDB.addAof = func(line CmdLine) {
				// 将 AOF 命令行写入 AOF 文件
				mdb.aofHandler.AddAof(singleDB.index, line)
			}
		}
	}
	return mdb
}

// Exec 执行客户端发送的命令
// 根据客户端发送的命令行，选择对应的数据库执行命令
func (mdb *StandaloneDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	// 使用 defer 捕获 panic 错误，防止程序崩溃
	defer func() {
		if err := recover(); err != nil {
			// 记录警告日志并返回未知错误回复
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	// 获取命令名称，转换为小写
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		// 处理 select 命令
		if len(cmdLine) != 2 {
			// 参数错误时返回错误回复
			return reply.MakeArgNumErrReply("select")
		}
		// 执行 select 命令，选择数据库
		return execSelect(c, mdb, cmdLine[1:])
	}
	// 普通命令处理
	dbIndex := c.GetDBIndex() // 获取客户端当前选择的数据库索引
	// 如果索引超出范围，返回错误
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	// 获取客户端选择的数据库实例
	selectedDB := mdb.dbSet[dbIndex]
	// 执行该数据库的命令
	return selectedDB.Exec(c, cmdLine)
}

// Close 关闭 StandaloneDatabase 实例，进行资源清理
func (mdb *StandaloneDatabase) Close() {
	// 关闭操作可以在这里进行（此示例没有具体实现）
}

// AfterClientClose 客户端连接关闭后的回调函数
func (mdb *StandaloneDatabase) AfterClientClose(c resp.Connection) {
	// 客户端关闭后的操作（此示例没有具体实现）
}

// execSelect 处理 select 命令，选择数据库
func execSelect(c resp.Connection, mdb *StandaloneDatabase, args [][]byte) resp.Reply {
	// 将数据库索引参数转换为整数
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		// 如果转换失败，返回无效的数据库索引错误
		return reply.MakeErrReply("ERR invalid DB index")
	}
	// 检查数据库索引是否在合法范围内
	if dbIndex >= len(mdb.dbSet) {
		// 如果索引超出范围，返回错误
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	// 选择指定的数据库
	c.SelectDB(dbIndex)
	// 返回成功回复
	return reply.MakeOkReply()
}
