package configure

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

func LoadConfig(configPath, configType string) *Config {
	once.Do(func() {
		viper.AddConfigPath(configPath)
		viper.SetConfigType(configType)
		viper.SetConfigName("config")

		if err := viper.ReadInConfig(); err != nil {
			log.Fatalln("cannot read configure: ", err)
		}

		if err := viper.Unmarshal(&config); err != nil {
			log.Fatalln("failed to unmarshal from configure: ", err)
		}

		viper.WatchConfig()

		viper.OnConfigChange(func(e fsnotify.Event) {
			if err := viper.Unmarshal(&config); err != nil {
				log.Fatalln("failed to unmarshal from configure: ", err)
			}
		})
	})

	return &config
}
