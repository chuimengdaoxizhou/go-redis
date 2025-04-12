package dict

// redis的字典实现

// Consumer是一个函数类型，接收一个键和一个值作为参数
//type Consumer func(key string, val interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool)        // 获取值
	Len() int                                             // 返回字典的长度
	Put(key string, val interface{}) (result int)         // 添加或更新值
	PutIfAbsent(key string, val interface{}) (result int) // 如果key不存在则添加值
	PutIfExists(key string, val interface{}) (result int) // 如果key存在则更新值
	Remove(key string) (result int)                       // 删除key
	ForEach(consumer Consumer)                            // 遍历字典
	Keys() []string                                       // 返回所有的key
	RandomKeys(limit int) []string                        // 随机返回指定数量的key
	RandomDistinctKeys(limit int) []string                // 随机返回指定数量的key，要求不重复
	Clear()                                               // 清空字典
}
