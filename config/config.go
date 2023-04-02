package config

import (
	"log"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var (
	once   sync.Once
	config Config
)

type TypeAppnedFsync string

const (
	TypeAppendFsyncAlways      TypeAppnedFsync = "always"
	TypeAppendFsyncEverySecond TypeAppnedFsync = "everysec"
	TypeAppendFsyncNever       TypeAppnedFsync = "no"
)

type Config struct {
	Host            string          `mapstructure:"bind"`
	Port            uint16          `mapstructure:"port"`
	DatabaseNum     int             `mapstructure:"databases"`
	RequirePassword string          `mapstructure:"requirepass"`
	AppnedOnly      bool            `mapstructure:"appendonly"`
	AppendFilename  string          `mapstructure:"appendfilename"`
	AppendFsync     TypeAppnedFsync `mapstructure:"appendfsync"`
}

func LoadConfig(configFile, configType string) *Config {
	once.Do(func() {
		viper.SetConfigFile(configFile)
		viper.SetConfigType(configType)

		if err := viper.ReadInConfig(); err != nil {
			log.Fatalln("cannot read config:", err)
		}

		if err := viper.Unmarshal(&config); err != nil {
			log.Fatalln("failed to unmarshal from config:", err)
		}

		viper.WatchConfig()

		viper.OnConfigChange(func(e fsnotify.Event) {
			if err := viper.Unmarshal(&config); err != nil {
				log.Fatalln("failed to unmarshal from config:", err)
			}
		})
	})

	return &config
}
