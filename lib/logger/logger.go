package logger

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"time"
)

// Settings 用于配置日志的设置
type Settings struct {
	Path       string
	Name       string
	Ext        string
	TimeFormat string
}

// Setup 用于设置日志
func Setup(settings *Settings) error {
	// 如果目录不存在，尝试创建
	if err := os.MkdirAll(settings.Path, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	// 获取当前日期
	currentDate := time.Now().Format(settings.TimeFormat)
	logFileName := fmt.Sprintf("%s_%s.%s", settings.Name, currentDate, settings.Ext)
	logFilePath := filepath.Join(settings.Path, logFileName)

	// 打开日志文件，追加写入
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}

	// 创建 Logrus 实例
	log := logrus.New()

	// 设置日志输出为文件
	log.SetOutput(logFile)

	// 设置日志格式，可以选择 JSON 格式或文本格式
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: settings.TimeFormat, // 使用传入的时间格式
	})

	// 设置日志级别
	log.SetLevel(logrus.InfoLevel)

	// 让 logrus 使用自定义的 logger
	logrus.SetOutput(logFile)

	// 可以在程序开始时输出一条日志，表示日志系统已经初始化
	log.Info("Logging setup complete.")

	return nil
}
