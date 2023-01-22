package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IfanTsai/metis/server"
)

func main() {
	listenFd, err := server.CreateTCPServer("0.0.0.0", 8080)
	if err != nil {
		log.Fatalf("failed to create tcp server: %v", err)
	}
	defer listenFd.Close()

	clientFd, err := listenFd.Accept()
	if err != nil {
		log.Printf("failed to accept: %v", err)

		return
	}

	buf := make([]byte, 1024)
	go func() {
		for {
			_, err = clientFd.Read(buf)
			if err != nil {
				log.Printf("failed to read: %v", err)

				return
			}

			clientFd.Write(buf)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}
