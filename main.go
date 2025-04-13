package main

import (
	"fmt"
	"goredis/config"
	"goredis/lib/logger"
	"goredis/resp/handler"
	"goredis/tcp"
	"os"
)

const configFile string = "redis.conf" //记录集群端口信息

var defaultProperties = &config.ServerProperties{
	Bind: "0.0.0.0",
	Port: 6379,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "goredis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})

	if fileExists(configFile) {
		config.SetupConfig(configFile)
	} else {
		config.Properties = defaultProperties
	}

	err := tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Address: fmt.Sprintf("%s:%d",
				config.Properties.Bind,
				config.Properties.Port),
		},
		handler.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}

// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
