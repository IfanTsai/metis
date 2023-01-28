package server

import (
	"log"
	"syscall"

	"github.com/IfanTsai/metis/ae"
	"github.com/IfanTsai/metis/socket"
	"github.com/pkg/errors"
)

const MaxBulk = 1024 * 4

type Server struct {
	fd        socket.FD
	clients   map[socket.FD]*Client
	eventLoop *ae.EventLoop
}

func NewServer() *Server {
	return &Server{
		clients: make(map[socket.FD]*Client),
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
