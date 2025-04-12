package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"goredis/config"
	"goredis/lib/logger"
	"goredis/resp/handler"
	"goredis/tcp"
	"os"
)

const configFile string = "redis.conf"

var defaultProperties = &config.ServerProperties{
	Bind: "0.0.0.0",
	Port: 6379,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	// 配置日志设置
	settings := &logger.Settings{
		Path:       "logs",       // 日志存放目录
		Name:       "goredis",    // 日志文件名
		Ext:        "log",        // 日志扩展名
		TimeFormat: "2006-01-02", // 日期格式
	}

	// 检查配置文件是否存在
	if fileExists(configFile) {
		if err := config.SetupConfig(configFile); err != nil {
			log.Fatalf("Error setting up config: %v", err)
		}
	} else {
		config.Properties = defaultProperties
	}
	// 初始化日志系统
	if err := logger.Setup(settings); err != nil {
		log.Fatalf("Error setting up logger: %v", err)
	}
	// 记录日志
	log.Infof("Application started with Bind: %s, Port: %d", config.Properties.Bind, config.Properties.Port)
	err := tcp.ListenAndServerWithSignal(
		&tcp.Config{
			Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		},
		handler.MakeHandler(),
	)
	if err != nil {
		log.Error(err)
	}
}

// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
