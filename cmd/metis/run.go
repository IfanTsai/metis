package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IfanTsai/metis/configure"
	"github.com/IfanTsai/metis/server"
)

func main() {
	config := configure.LoadConfig("./", "toml")
	metisServer := server.NewServer()

	go func() {
		if err := metisServer.Run(config.Host, config.Port); err != nil {
			log.Panicln("metis server run error: ", err)
		}
	}()

	log.Printf("metis server is running on %s:%d", config.Host, config.Port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	metisServer.Stop()
}
