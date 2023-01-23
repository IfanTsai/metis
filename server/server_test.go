package server_test

import (
	"net"
	"syscall"
	"testing"

	"github.com/IfanTsai/metis/server"
	"github.com/IfanTsai/metis/socket"
	"github.com/stretchr/testify/require"
)

func TestCreateTCPServer(t *testing.T) {
	startCh := make(chan struct{})
	connectedCh := make(chan struct{})
	endCh := make(chan struct{})

	port, err := getRandomAvailablePort()
	require.NoError(t, err)

	go runEchoTCPSever(t, port, startCh, connectedCh, endCh)

	<-startCh

	fd, err := socket.Socket(syscall.AF_INET, syscall.SOCK_STREAM)
	require.NoError(t, err)
	defer fd.Close()

	err = fd.Connect("127.0.0.1", port)
	require.NoError(t, err)

	connectedCh <- struct{}{}

	writeBuf := []byte("Hello, world!")
	nWrite, err := fd.Write(writeBuf)
	require.NoError(t, err)
	require.Equal(t, len(writeBuf), nWrite)

	readBuf := make([]byte, 1024)
	nRead, err := fd.Read(readBuf)
	require.NoError(t, err)
	require.Equal(t, len(writeBuf), nRead)
	require.Equal(t, writeBuf, readBuf[:nRead])

	endCh <- struct{}{}
}

func runEchoTCPSever(t *testing.T, port uint16, startCh chan<- struct{}, connectedCh, endCh <-chan struct{}) {
	listenFd, err := server.CreateTCPServer("127.0.0.1", port)
	require.NoError(t, err)
	defer listenFd.Close()

	startCh <- struct{}{}
	<-connectedCh

	clientFd, err := listenFd.Accept()
	require.NoError(t, err)

	buf := make([]byte, 1024)
	nRead, err := clientFd.Read(buf)
	require.NoError(t, err)

	nWrite, err := clientFd.Write(buf[:nRead])
	require.NoError(t, err)
	require.Equal(t, nRead, nWrite)

	<-endCh
}

func getRandomAvailablePort() (uint16, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return uint16(l.Addr().(*net.TCPAddr).Port), nil
}
