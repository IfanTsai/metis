package server

import (
	"log"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IfanTsai/go-lib/utils/byteutils"
	"github.com/IfanTsai/metis/ae"
	"github.com/IfanTsai/metis/config"
	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/socket"
	"github.com/pkg/errors"
)

const (
	defaultDBNum          = 16
	maxBulk               = 1024 * 4
	checkExpireEntryCount = 100
	serverCronInterval    = 1
)

type TypeBackgroundTask uint8

const (
	TypeBackgroundTaskNone TypeBackgroundTask = iota
	TypeBackgroundTaskRDB
	TypeBackgroundTaskAOFRewrite
)

type Server struct {
	host            string
	port            uint16
	fd              socket.FD
	clients         map[socket.FD]*Client
	eventLoop       *ae.EventLoop
	dbs             []*database.Databse
	requirePassword string
	dirty           int64 // changes to DB from the last save

	backgroundTaskTypeAtomic atomic.Uint32

	// AOF persistence
	aofEnable        bool
	aofFilename      string
	aofFsync         config.TypeAppnedFsync
	aofLastFsyncTime time.Time
	aofSelectDBID    int
	aofFile          *os.File
	aofBuf           strings.Builder
	aofCurrentSize   uint

	aofRewritePercent  uint
	aofRewriteMinSize  uint
	aofRewriteBaseSize uint
	aofRewriteDoneCh   chan string // tmp aof filename
	aofRewriteBuf      strings.Builder
}

func NewServer(config *config.Config) *Server {
	dbNum := defaultDBNum
	if config.DatabaseNum > 0 {
		dbNum = config.DatabaseNum
	}

	server := &Server{
		host:              config.Host,
		port:              config.Port,
		clients:           make(map[socket.FD]*Client),
		requirePassword:   config.RequirePassword,
		aofEnable:         config.AofEnable,
		aofFilename:       config.AofFilename,
		aofFsync:          config.AofFsync,
		aofRewritePercent: config.AofRewritePercent,
		aofRewriteMinSize: config.AofRewriteMinSize,
		aofRewriteDoneCh:  make(chan string, 1),
	}

	server.backgroundTaskTypeAtomic.Store(uint32(TypeBackgroundTaskNone))

	if server.aofEnable {
		appendFile, err := os.OpenFile(server.aofFilename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("failed to open append only file: %+v", err)
		}

		fileInfo, err := appendFile.Stat()
		if err != nil {
			log.Fatalf("failed to get append only file info: %+v", err)
		}

		server.aofCurrentSize = uint(fileInfo.Size())
		server.aofFile = appendFile
	}

	server.dbs = make([]*database.Databse, dbNum)
	for i := 0; i < dbNum; i++ {
		server.dbs[i] = database.NewDatabase(i)
	}

	return server
}

func (s *Server) Run() error {
	listenFd, err := CreateTCPServer(s.host, s.port)
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

	if err := eventLoop.AddTimeEvent(ae.TypeTimeEventNormal, serverCronInterval, serverCron, s); err != nil {
		return err
	}

	eventLoop.SetBeforeSleepProc(beforeSleepProc(eventLoop, s))

	s.fd = listenFd
	s.eventLoop = eventLoop

	if s.aofEnable {
		loadAppendOnlyFile(s)
	}

	return eventLoop.Main()
}

func (s *Server) Stop() {
	s.eventLoop.Stop()
	s.fd.Close()
	close(s.aofRewriteDoneCh)

	if s.aofFile != nil {
		s.aofFile.Close()
	}
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

func serverCron(el *ae.EventLoop, id int64, clientData any) {
	srv := clientData.(*Server)

	databasesCron(srv)

	if srv.backgroundTaskTypeAtomic.Load() == uint32(TypeBackgroundTaskNone) &&
		srv.aofRewritePercent > 0 && srv.aofCurrentSize > srv.aofRewriteMinSize {
		base := srv.aofRewriteBaseSize
		if base == 0 {
			base = 1
		}

		growth := (srv.aofCurrentSize * 100 / base) - 100
		if growth >= srv.aofRewritePercent {
			rewriteAppendOnlyFileBackground(srv)
		}
	}

	if len(srv.aofRewriteDoneCh) > 0 {
		aofRewriteDoneCallback(srv)
	}
}

func databasesCron(srv *Server) {
	// expire keys by random sampling.
	for _, db := range srv.dbs {
		for i := 0; i < checkExpireEntryCount; i++ {
			entry := db.Expire.GetRandomKey()
			if entry == nil {
				break
			}

			when, ok := entry.Value.(int64)
			if !ok {
				log.Printf("invalid expire value: %v, key: %v\n", entry.Value, entry.Key)
				continue
			}

			if when < time.Now().UnixMilli() {
				db.Dict.Delete(entry.Key)
				db.Expire.Delete(entry.Key)
			}
		}
	}
}

func beforeSleepProc(el *ae.EventLoop, srv *Server) ae.BeforeSleepProc {
	return func(el *ae.EventLoop) {
		// write the AOF buffer on disk.
		flushAppendOnlyFile(srv)
	}
}
