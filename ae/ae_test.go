package ae_test

import (
	"syscall"
	"testing"
	"time"

	"github.com/IfanTsai/metis/ae"
	"github.com/IfanTsai/metis/server"
	"github.com/IfanTsai/metis/socket"
	"github.com/stretchr/testify/require"
)

func TestEventLoop_Main(t *testing.T) {
	eventLoop, err := ae.NewEventLoop()
	require.NoError(t, err)

	listenFd, err := server.CreateTCPServer("127.0.0.1", 0)
	require.NoError(t, err)

	err = eventLoop.AddFileEvent(listenFd, ae.TypeFileEventReadable,
		func(el *ae.EventLoop, fd socket.FD, clientData any) {
			clientFd, err := fd.Accept()
			require.NoError(t, err)

			err = el.AddFileEvent(clientFd, ae.TypeFileEventReadable,
				func(el *ae.EventLoop, fd socket.FD, clientData any) {
					buf := make([]byte, 1024)
					_, err := fd.Read(buf)
					require.NoError(t, err)

					err = eventLoop.AddFileEvent(clientFd, ae.TypeFileEventWritable,
						func(el *ae.EventLoop, fd socket.FD, clientData any) {
							buf := []byte("hello world")
							n, err := fd.Write(buf)
							require.NoError(t, err)
							require.Equal(t, len(buf), n)

							err = eventLoop.RemoveFileEvent(clientFd, ae.TypeFileEventWritable)
							require.NoError(t, err)
						}, nil)
					require.NoError(t, err)
				}, nil)
			require.NoError(t, err)
		}, nil)
	require.NoError(t, err)

	go eventLoop.Main()

	addr, err := listenFd.GetSockName()
	require.NoError(t, err)

	fd, err := socket.Socket(syscall.AF_INET, syscall.SOCK_STREAM)
	require.NoError(t, err)

	err = fd.Connect("127.0.0.1", uint16(addr.Port))
	require.NoError(t, err)

	writeBuf := []byte("hello world")
	n, err := fd.Write(writeBuf)
	require.NoError(t, err)
	require.Equal(t, len(writeBuf), n)

	readBuf := make([]byte, 1024)
	n, err = fd.Read(readBuf)
	require.NoError(t, err)
	require.Equal(t, len(writeBuf), n)

	err = eventLoop.AddTimeEvent(ae.TypeTimeEventOnce, 10, func(el *ae.EventLoop, id int64, clientData any) {
		require.Equal(t, int64(0), id)
	}, nil)
	require.NoError(t, err)

	stop := make(chan struct{}, 10*2)

	timeEventCalled := 0
	err = eventLoop.AddTimeEvent(ae.TypeTimeEventNormal, 10, func(el *ae.EventLoop, id int64, clientData any) {
		require.Equal(t, int64(1), id)

		if timeEventCalled == 10 {
			return
		}

		timeEventCalled++
		stop <- struct{}{}
	}, nil)
	require.NoError(t, err)

	beforeSleepCalled := 0
	eventLoop.SetBeforeSleepProc(func(el *ae.EventLoop) {
		if beforeSleepCalled == 10 {
			return
		}

		beforeSleepCalled++
		stop <- struct{}{}
	})

	for i := 0; i < cap(stop); i++ {
		<-stop
	}

	eventLoop.Stop()

	time.Sleep(time.Millisecond * 200)
}
