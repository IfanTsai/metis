package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IfanTsai/metis/ae"
	"github.com/IfanTsai/metis/server"
	"github.com/IfanTsai/metis/socket"
)

func main() {
	listenFd, err := server.CreateTCPServer("0.0.0.0", 8080)
	if err != nil {
		log.Fatalf("failed to create tcp server: %+v", err)
	}
	defer listenFd.Close()

	eventLoop, err := ae.NewEventLoop()
	if err != nil {
		log.Printf("failed to create event loop: %+v", err)
	}

	if err := eventLoop.AddFileEvent(listenFd, ae.TypeFileEventReadable,
		func(el *ae.EventLoop, fd socket.FD, clientData any) {
			clientFd, err := listenFd.Accept()
			if err != nil {
				log.Printf("failed to accept: %v", err)

				return
			}

			if err := eventLoop.AddFileEvent(clientFd, ae.TypeFileEventReadable,
				func(el *ae.EventLoop, fd socket.FD, clientData any) {
					buf := make([]byte, 1024)
					n, err := clientFd.Read(buf)
					if n == 0 {
						eventLoop.RemoveFileEvent(clientFd, ae.TypeFileEventReadable)
						clientFd.Close()

						return
					}

					if err != nil {
						log.Printf("failed to read: %v", err)

						return
					}

					clientFd.Write(buf)
				}, nil); err != nil {
				log.Printf("failed to add file event: %+v", err)

				return
			}
		}, nil); err != nil {
		log.Printf("failed to add file event: %+v", err)
	}

	go eventLoop.Main()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	eventLoop.Stop()
}
