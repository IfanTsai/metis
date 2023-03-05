package server

import (
	"bytes"
	"container/list"
	"fmt"
	"strconv"
	"strings"

	"github.com/IfanTsai/go-lib/utils/byteutils"
	"github.com/IfanTsai/metis/ae"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/IfanTsai/metis/socket"
	"github.com/pkg/errors"
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
	args         []string
	multiBulkLen int
	bulkLen      int
	replayHead   *list.List // string
	sentLen      int
}

func NewClient(srv *Server, fd socket.FD) *Client {
	return &Client{
		srv:        srv,
		fd:         fd,
		queryBuf:   make([]byte, maxBulk),
		replayHead: list.New(),
	}
}

func (c *Client) getCRLFIndexFromQueryBuffer() int {
	return bytes.Index(c.queryBuf[:c.queryLen], []byte("\r\n"))
}

func (c *Client) getNumFromQueryBuffer(indexCRLF int) (int, error) {
	return strconv.Atoi(byteutils.B2S(c.queryBuf[1:indexCRLF]))
}

func (c *Client) moveToNextLineInQueryBuffer(indexCRLF int) {
	c.queryBuf = c.queryBuf[indexCRLF+2:]
	c.queryLen -= indexCRLF + 2
}

func (c *Client) addReply(str string) error {
	if c.replayHead.Len() == 0 {
		if err := c.srv.eventLoop.AddFileEvent(c.fd, ae.TypeFileEventWritable, sendReplayToClient, c); err != nil {
			return errors.Wrap(err, "failed to add writable file event")
		}
	}

	c.replayHead.PushBack(str)

	return nil
}

func (c *Client) addReplyString(str string) error {
	return c.addReply(str)
}

func (c *Client) addReplyStringf(format string, args ...any) error {
	return c.addReplyString(fmt.Sprintf(format, args...))
}

func (c *Client) addReplyZsetElements(elements []*datastruct.ZsetElement, withScoreIndex int) error {
	withScore := false
	if withScoreIndex > 0 && len(c.args) > withScoreIndex {
		if strings.EqualFold(c.args[withScoreIndex], "WITHSCORES") {
			withScore = true
		} else {
			c.addReplyString("-ERR invalid option: " + c.args[withScoreIndex] + "\r\n")
			return nil
		}
	}
	if withScore {
		c.addReplyStringf("*%d\r\n", len(elements)*2)
	} else {
		c.addReplyStringf("*%d\r\n", len(elements))
	}

	for _, element := range elements {
		c.addReplyStringf("$%d\r\n%s\r\n", len(element.Member), element.Member)
		if withScore {
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			c.addReplyStringf("$%d\r\n%s\r\n", len(scoreStr), scoreStr)
		}
	}

	return nil
}

func (c *Client) free() {
	if c.srv != nil {
		delete(c.srv.clients, c.fd)

		if c.srv.eventLoop != nil {
			c.srv.eventLoop.RemoveFileEvent(c.fd, ae.TypeFileEventReadable)
			c.srv.eventLoop.RemoveFileEvent(c.fd, ae.TypeFileEventWritable)
		}
	}

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
