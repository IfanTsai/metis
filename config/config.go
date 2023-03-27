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

type Config struct {
	Host        string `mapstructure:"bind"`
	Port        uint16 `mapstructure:"port"`
	DatabaseNum int    `mapstructure:"databases"`
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
