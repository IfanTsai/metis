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

func (c *Client) addReplySimpleString(str string) error {
	return c.addReplyStringf("+%s\r\n", str)
}

func (c *Client) addReplyBulkString(str string) error {
	return c.addReplyStringf("$%d\r\n%s\r\n", len(str), str)
}

func (c *Client) addReplyInt(num int64) error {
	return c.addReplyStringf(":%d\r\n", num)
}

func (c *Client) addReplyError(err string) error {
	return c.addReplyStringf("-ERR %s\r\n", err)
}

func (c *Client) addReplyErrorf(format string, args ...any) error {
	return c.addReplyError(fmt.Sprintf(format, args...))
}

func (c *Client) addReplyOK() error {
	return c.addReplyString("+OK\r\n")
}

func (c *Client) addReplyNull() error {
	return c.addReplyString("$-1\r\n")
}

func (c *Client) addReplyArrays(strs []string) error {
	if err := c.addReplyStringf("*%d\r\n", len(strs)); err != nil {
		return err
	}

	for _, str := range strs {
		if err := c.addReplyBulkString(str); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) addReplyZsetElements(elements []*datastruct.ZsetElement, withScoreIndex int) error {
	withScore := false
	if withScoreIndex > 0 && len(c.args) > withScoreIndex {
		if strings.EqualFold(c.args[withScoreIndex], "WITHSCORES") {
			withScore = true
		} else {
			return c.addReplyError("invalid option: %s" + c.args[withScoreIndex])
		}
	}

	var respArrayLen int
	if withScore {
		respArrayLen = len(elements) * 2
	} else {
		respArrayLen = len(elements)
	}

	if err := c.addReplyStringf("*%d\r\n", respArrayLen); err != nil {
		return err
	}

	for _, element := range elements {
		if err := c.addReplyBulkString(element.Member); err != nil {
			return err
		}

		if withScore {
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			if err := c.addReplyBulkString(scoreStr); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Client) addReplySet(set *datastruct.Set) error {
	if err := c.addReplyStringf("*%d\r\n", set.Size()); err != nil {
		return err
	}

	for _, member := range set.Range() {
		if err := c.addReplyBulkString(member.(string)); err != nil {
			return err
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
