package log

import (
	"sync"

	"github.com/IfanTsai/go-lib/logger"
	"github.com/IfanTsai/metis/config"
	"go.uber.org/zap"
)

var (
	once sync.Once

	Debug func(string, ...zap.Field)
	Info  func(string, ...zap.Field)
	Warn  func(string, ...zap.Field)
	Error func(string, ...zap.Field)
	Fatal func(string, ...zap.Field)
	Panic func(string, ...zap.Field)
)

func InitLogger(cfg *config.Config) {
	once.Do(func() {
		var opts []logger.Option

		if cfg.LogFilepath != "" {
			opts = append(opts, logger.WithFileRotationP(cfg.LogFilepath))
		}

		switch cfg.LogLevel {
		case config.LogLevelDebug:
			opts = append(opts, logger.WithDebugLevel())
		case config.LogLevelInfo:
			opts = append(opts, logger.WithInfoLevel())
		case config.LogLevelWarn:
			opts = append(opts, logger.WithWarnLevel())
		case config.LogLevelError:
			opts = append(opts, logger.WithErrorLevel())
		default:
			opts = append(opts, logger.WithInfoLevel())
		}

		metisLogger := logger.NewJSONLogger(opts...)

		Debug = metisLogger.Debug
		Info = metisLogger.Info
		Warn = metisLogger.Warn
		Error = metisLogger.Error
		Fatal = metisLogger.Fatal
		Panic = metisLogger.Panic
	})
}
