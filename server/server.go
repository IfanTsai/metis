package server

import (
	"log"
	"syscall"

	"github.com/IfanTsai/metis/ae"
	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/IfanTsai/metis/socket"
	"github.com/pkg/errors"
)

const MaxBulk = 1024 * 4

type Server struct {
	fd        socket.FD
	clients   map[socket.FD]*Client
	eventLoop *ae.EventLoop
	db        *database.Databse
}

func NewServer() *Server {
	return &Server{
		clients: make(map[socket.FD]*Client),
		db:      database.NewDatabase(),
	}
}

func (s *Server) Run(ip string, port uint16) error {
	listenFd, err := CreateTCPServer(ip, port)
	if err != nil {
		log.Fatalf("failed to create tcp server: %+v", err)
	}
	defer listenFd.Close()

	eventLoop, err := ae.NewEventLoop()
	if err != nil {
		return err
	}

	if err := eventLoop.AddFileEvent(listenFd, ae.TypeFileEventReadable, acceptTCPHandler, s); err != nil {
		return err
	}

	s.fd = listenFd
	s.eventLoop = eventLoop

	return eventLoop.Main()
}

func (s *Server) Stop() {
	s.eventLoop.Stop()
	s.fd.Close()
}

func acceptTCPHandler(el *ae.EventLoop, fd socket.FD, extra any) {
	clientFd, err := fd.Accept()
	if err != nil {
		log.Printf("failed to accept: %v", err)

		return
	}

	if err := clientFd.SetNonBlock(); err != nil {
		log.Printf("failed to set non block: %v", err)
		clientFd.Close()

		return
	}

	srv := extra.(*Server)
	client := NewClient(srv, clientFd)

	if err := el.AddFileEvent(clientFd, ae.TypeFileEventReadable, readQueryFromClient, client); err != nil {
		log.Printf("failed to add file event: %v", err)
		client.free()

		return
	}

	srv.clients[clientFd] = client
}

func readQueryFromClient(el *ae.EventLoop, fd socket.FD, clientData any) {
	client := clientData.(*Client)
	if len(client.queryBuf)-client.queryLen < MaxBulk {
		client.queryBuf = append(client.queryBuf, make([]byte, MaxBulk)...)
	}

	nRead, err := fd.Read(client.queryBuf[client.queryLen:])
	if err != nil {
		switch errors.Cause(err).(syscall.Errno) {
		case syscall.EAGAIN, syscall.EINTR:
			return
		case syscall.ECONNRESET:
			nRead = 0
		default:
			log.Printf("read error: %v", err)

			return
		}
	}

	if nRead == 0 {
		client.free()

		return
	}

	client.queryLen += nRead
	if err := processInputBuffer(client); err != nil {
		log.Printf("process input buffer error: %v", err)
		client.free()

		return
	}
}

func sendReplayToClient(el *ae.EventLoop, fd socket.FD, clientData any) {
	client := clientData.(*Client)
	for client.replayHead.Len() > 0 {
		element := client.replayHead.Front()
		replayObject := element.Value.(*datastruct.Object)
		buf := []byte(replayObject.Value.(string))
		if client.sentLen < len(buf) {
			nWritten, err := client.fd.Write(buf[client.sentLen:])
			if err != nil {
				switch errors.Cause(err).(syscall.Errno) {
				case syscall.EAGAIN, syscall.EINTR:
				default:
					log.Printf("write error: %v", err)
					client.free()
				}

				return
			}

			client.sentLen += nWritten
			if client.sentLen == len(buf) {
				client.sentLen = 0
				client.replayHead.Remove(element)
			} else {
				break
			}
		}
	}

	if client.replayHead.Len() == 0 {
		if err := el.RemoveFileEvent(client.fd, ae.TypeFileEventWritable); err != nil {
			log.Printf("failed to remove file event: %v", err)

			return
		}
	}
}
