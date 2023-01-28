package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IfanTsai/metis/server"
)

const (
	ip   = "0.0.0.0"
	port = 8080
)

func main() {
	metisServer := server.NewServer()

	go func() {
		if err := metisServer.Run("0.0.0.0", 8080); err != nil {
			panic(err)
		}
	}()

	log.Printf("metis server is running on %s:%d", ip, port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	metisServer.Stop()
}
