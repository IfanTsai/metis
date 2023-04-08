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
	Host              string          `mapstructure:"bind"`
	Port              uint16          `mapstructure:"port"`
	DatabaseNum       int             `mapstructure:"databases"`
	RequirePassword   string          `mapstructure:"requirepass"`
	AofEnable         bool            `mapstructure:"appendonly"`
	AofFilename       string          `mapstructure:"appendfilename"`
	AofFsync          TypeAppnedFsync `mapstructure:"appendfsync"`
	AofRewritePercent uint            `mapstructure:"auto-aof-rewrite-percentage"`
	AofRewriteMinSize uint            // `mapstructure:"auto-aof-rewrite-min-size"`
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

		config.AofRewriteMinSize = viper.GetSizeInBytes("auto-aof-rewrite-min-size")

		viper.WatchConfig()

		viper.OnConfigChange(func(e fsnotify.Event) {
			if err := viper.Unmarshal(&config); err != nil {
				log.Fatalln("failed to unmarshal from config:", err)
			}

			config.AofRewriteMinSize = viper.GetSizeInBytes("auto-aof-rewrite-min-size")
		})
	})

	return &config
}
