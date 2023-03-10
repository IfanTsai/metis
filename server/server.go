package server

import (
	"log"
	"syscall"
	"time"

	"github.com/IfanTsai/go-lib/utils/byteutils"
	"github.com/IfanTsai/metis/ae"
	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/socket"
	"github.com/pkg/errors"
)

const (
	maxBulk               = 1024 * 4
	checkExpireEntryCount = 100
	checkExpireInterval   = 100
)

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

	if err := eventLoop.AddTimeEvent(ae.TypeTimeEventNormal, checkExpireInterval, expireKeyCronJob, s); err != nil {
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
	if len(client.queryBuf)-client.queryLen < maxBulk {
		client.queryBuf = append(client.queryBuf, make([]byte, maxBulk)...)
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
		buf := byteutils.S2B(element.Value.(string))
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

func expireKeyCronJob(el *ae.EventLoop, id int64, clientData any) {
	srv := clientData.(*Server)
	for i := 0; i < checkExpireEntryCount; i++ {
		entry := srv.db.Expire.GetRandomKey()
		if entry == nil {
			break
		}

		when, ok := entry.Value.(int64)
		if !ok {
			log.Printf("invalid expire value: %v, key: %v\n", entry.Value, entry.Key)
			continue
		}

		if when < time.Now().UnixMilli() {
			srv.db.Dict.Delete(entry.Key)
			srv.db.Expire.Delete(entry.Key)
		}
	}
}
