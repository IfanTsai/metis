package cmd

import (
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/IfanTsai/metis/config"
	"github.com/IfanTsai/metis/server"
	"github.com/spf13/cobra"
)

var configFile string

var rootCmd = &cobra.Command{
	Use:   "metis",
	Short: "metis is a simple Redis server clone written in Golang",
	Run: func(cmd *cobra.Command, args []string) {
		run(configFile)
	},
}

func init() {
	rootCmd.Flags().StringVarP(
		&configFile, "config", "c", "./config.toml",
		"config file (default is ./config.toml)")
}

func run(configFile string) {
	cfg := config.LoadConfig(configFile, filepath.Ext(configFile)[1:])
	metisServer := server.NewServer(cfg)

	go func() {
		if err := metisServer.Run(); err != nil {
			log.Panicln("metis server run error: ", err)
		}
	}()

	log.Printf("metis server is running on %s:%d", cfg.Host, cfg.Port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	metisServer.Stop()
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
