package dict

import "sync"

type SyncDict struct {
	m sync.Map
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m.Load(key)
	return val, ok
}

func (dict *SyncDict) Len() int {
	count := 0
	dict.m.Range(func(key, value interface{}) bool { // Range将传入的函数应用于每个键值对
		count++
		return true
	})
	return count
}

func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	// 先尝试加载，如果不存在则添加
	_, existed := dict.m.Load(key)
	dict.m.Store(key, val)
	if existed {
		return 0 // 更新的
	}
	return 1 // 插入新的
}

func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	// 先尝试加载，如果不存在则添加
	_, existed := dict.m.Load(key)
	if existed { // 如果存在则不添加
		return 0
	}
	dict.m.Store(key, val)
	return 1 // 插入新的
}

func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	// 先尝试加载，如果不存在则添加
	_, existed := dict.m.Load(key)
	if !existed { // 如果不存在则不添加
		return 0
	}
	dict.m.Store(key, val)
	return 1 // 更新的
}

func (dict *SyncDict) Remove(key string) (result int) {
	// 先尝试加载
	_, existed := dict.m.Load(key)
	if !existed { // 如果不存在
		return 0
	}
	dict.m.Delete(key)
	return 1 // 删除的
}

// Consumer是一个函数类型，接收一个键和一个值作为参数
type Consumer func(key string, val interface{}) bool

func (dict *SyncDict) ForEach(consumer Consumer) {
	dict.m.Range(func(key, value interface{}) bool { // Range将传入的函数应用于每个键值对
		consumer(key.(string), value)
		return true
	})
}

func (dict *SyncDict) Keys() []string {
	keys := make([]string, 0)
	dict.m.Range(func(key, value interface{}) bool { // Range将传入的函数应用于每个键值对
		keys = append(keys, key.(string))
		return true
	})
	return keys
}

func (dict *SyncDict) RandomKeys(limit int) []string {
	result := make([]string, dict.Len())
	for i := 0; i < limit; i++ {
		dict.m.Range(func(key, value any) bool {
			result[i] = key.(string)
			return false
		})
	}
	return result
}

func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value any) bool {
		result[i] = key.(string)
		i++
		if i == limit {
			return false // 结束遍历
		}
		return true // 继续遍历
	})
	return result
}

func (dict *SyncDict) Clear() {
	*dict = *MakeSyncDict() // 清空字典 ,实际上是重新创建一个新的字典,旧的字典会被垃圾回收
}
