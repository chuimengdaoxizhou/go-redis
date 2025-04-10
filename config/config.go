package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type ServerProperties struct {
	Bind           string   `cfg:"bind"`
	Port           int      `cfg:"port"`
	AppendOnly     bool     `cfg:"appendOnly"`
	AppendFilename string   `cfg:"appendFilename"`
	MaxClients     int      `cfg:"maxclients"`
	RequirePass    string   `cfg:"requirepass"`
	Databases      int      `cfg:"databases"`
	Peers          []string `cfg:"peers"`
	Self           string   `cfg:"self"`
}

var Properties *ServerProperties

func SetupConfig(configFile string) error {
	file, err := os.Open(configFile)
	if err != nil {
		return fmt.Errorf("failed to open config file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	Properties = &ServerProperties{} // Initialize the Properties struct

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)

		// 忽略空行和注释行
		if len(line) == 0 || line[0] == '#' {
			continue
		}

		// 按空格分割键值对
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		key := parts[0]
		value := parts[1]

		switch key {
		case "bind":
			Properties.Bind = value
		case "port":
			fmt.Sscanf(value, "%d", &Properties.Port)
		default:
			// 其他字段
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read config file: %v", err)
	}

	return nil
}
