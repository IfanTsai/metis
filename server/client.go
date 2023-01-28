package server

import (
	"bytes"
	"strconv"

	"github.com/IfanTsai/metis/ae"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/IfanTsai/metis/socket"
)

type CommandType int

const (
	CommandTypeUnknown CommandType = iota
	CommandTypeInline
	CommandTypeBulk
	CommandTypeMultiBulk
)

type Client struct {
	srv          *Server
	fd           socket.FD
	queryBuf     []byte
	queryLen     int
	cmdType      CommandType
	args         []*datastruct.Object
	multiBulkLen int
	bulkLen      int
}

func NewClient(srv *Server, fd socket.FD) *Client {
	return &Client{
		srv:      srv,
		fd:       fd,
		queryBuf: make([]byte, MaxBulk),
	}
}

func (c *Client) getCRLFIndexFromQueryBuffer() int {
	return bytes.Index(c.queryBuf[:c.queryLen], []byte("\r\n"))
}

func (c *Client) getNumFromQueryBuffer(indexCRLF int) (int, error) {
	return strconv.Atoi(string(c.queryBuf[1:indexCRLF]))
}

func (c *Client) moveToNextLineInQueryBuffer(indexCRLF int) {
	c.queryBuf = c.queryBuf[indexCRLF+2:]
	c.queryLen -= indexCRLF + 2
}

func (c *Client) free() {
	if c.srv != nil && c.srv.eventLoop != nil {
		c.srv.eventLoop.RemoveFileEvent(c.fd, ae.TypeFileEventReadable)
		c.srv.eventLoop.RemoveFileEvent(c.fd, ae.TypeFileEventWritable)
	}

	delete(c.srv.clients, c.fd)
	c.fd.Close()
}

func (c *Client) reset() {
	c.queryBuf = nil
	c.queryLen = 0
	c.cmdType = CommandTypeUnknown
	c.args = nil
	c.multiBulkLen = 0
	c.bulkLen = 0
}
