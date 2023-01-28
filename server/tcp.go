package server

import (
	"syscall"

	"github.com/IfanTsai/metis/socket"
)

func CreateTCPServer(ip string, port uint16) (socket.FD, error) {
	fd, err := socket.Socket(syscall.AF_INET, syscall.SOCK_STREAM)
	if err != nil {
		return -1, err
	}

	if err := fd.SetSockOpt(syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		fd.Close()

		return -1, err
	}

	if err := fd.Bind(ip, port); err != nil {
		fd.Close()

		return -1, err
	}

	if err := fd.Listen(syscall.SOMAXCONN); err != nil {
		fd.Close()

		return -1, err
	}

	return fd, nil
}
