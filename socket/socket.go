package socket

import (
	"net"
	"syscall"

	"github.com/pkg/errors"
)

type FD int

func Socket(domain, typ int) (FD, error) {
	fd, err := syscall.Socket(domain, typ, 0)
	if err != nil {
		return -1, errors.Wrap(err, "failed to create socket")
	}

	return FD(fd), nil
}

func (fd FD) Close() error {
	if err := syscall.Close(int(fd)); err != nil {
		return errors.Wrap(err, "failed to close socket")
	}

	return nil
}

func (fd FD) Connect(ip string, port uint16) error {
	ipAddr := net.ParseIP(ip)
	if ipAddr == nil {
		return errors.New("invalid ip address")
	}

	addr := &syscall.SockaddrInet4{Port: int(port)}
	copy(addr.Addr[:], ipAddr)
	if err := syscall.Connect(int(fd), addr); err != nil {
		return errors.Wrap(err, "failed to connect socket")
	}

	return nil
}

func (fd FD) Bind(ip string, port uint16) error {
	ipAddr := net.ParseIP(ip)
	if ipAddr == nil {
		return errors.New("invalid ip address")
	}

	addr := &syscall.SockaddrInet4{Port: int(port)}
	copy(addr.Addr[:], ipAddr)
	if err := syscall.Bind(int(fd), addr); err != nil {
		return errors.Wrap(err, "failed to bind socket")
	}

	return nil
}

func (fd FD) Listen(backlog int) error {
	if err := syscall.Listen(int(fd), backlog); err != nil {
		return errors.Wrap(err, "failed to listen socket")
	}

	return nil
}

func (fd FD) Accept() (FD, error) {
	nfd, _, err := syscall.Accept4(int(fd), 0)
	if err != nil {
		return -1, errors.Wrap(err, "failed to accept socket")
	}

	return FD(nfd), nil
}

func (fd FD) Read(buf []byte) (int, error) {
	n, err := syscall.Read(int(fd), buf)
	if err != nil {
		return -1, errors.Wrap(err, "failed to read socket")
	}

	return n, nil
}

func (fd FD) Write(buf []byte) (int, error) {
	n, err := syscall.Write(int(fd), buf)
	if err != nil {
		return -1, errors.Wrap(err, "failed to write socket")
	}

	return n, nil
}

func (fd FD) SetSockOpt(level, opt, value int) error {
	if err := syscall.SetsockoptInt(int(fd), level, opt, value); err != nil {
		return errors.Wrap(err, "failed to set socket option")
	}

	return nil
}

func (fd FD) GetSockName() (*syscall.SockaddrInet4, error) {
	addr, err := syscall.Getsockname(int(fd))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get socket name")
	}

	return addr.(*syscall.SockaddrInet4), nil
}

func (fd FD) SetNonBlock() error {
	return syscall.SetNonblock(int(fd), true)
}
