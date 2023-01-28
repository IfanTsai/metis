package server

import (
	"strings"

	"github.com/IfanTsai/metis/datastruct"
	"github.com/pkg/errors"
)

const MaxInlineSize = 1024 * 64

// https://redis.io/docs/reference/protocol-spec/
func processInputBuffer(client *Client) error {
	for client.queryLen > 0 {
		if client.cmdType == CommandTypeUnknown {
			switch client.queryBuf[0] {
			case '*':
				client.cmdType = CommandTypeMultiBulk
			case '$':
				client.cmdType = CommandTypeBulk
			default:
				client.cmdType = CommandTypeInline
			}
		}

		var (
			ok  bool
			err error
		)

		switch client.cmdType {
		case CommandTypeInline:
			ok, err = processInlineBuffer(client)
		case CommandTypeBulk:
			ok, err = processBulkBuffer(client)
		case CommandTypeMultiBulk:
			ok, err = processMultiBulkBuffer(client)
		}

		if err != nil {
			return err
		}

		if ok {
			if len(client.args) > 0 {
				processCommand(client)
			} else {
				client.reset()
			}
		} else {
			break // wait for more data to arrive
		}
	}

	return nil
}

func processInlineBuffer(client *Client) (bool, error) {
	index := client.getCRLFIndexFromQueryBuffer()
	if index == -1 {
		if client.queryLen > MaxInlineSize {
			return false, errors.New("too big inline request")
		}

		return false, nil
	}

	subs := strings.Split(string(client.queryBuf[1:index]), " ")
	client.args = make([]*datastruct.Object, len(subs))
	for i, sub := range subs {
		client.args[i] = datastruct.NewObject(datastruct.ObjectTypeString, sub)
	}

	client.moveToNextLineInQueryBuffer(index)

	return true, nil
}

func processBulkBuffer(client *Client) (bool, error) {
	if client.bulkLen == 0 {
		index := client.getCRLFIndexFromQueryBuffer()
		if index == -1 {
			if client.queryLen > MaxInlineSize {
				return false, errors.New("too big inline request")
			}

			return false, nil
		}

		num, err := client.getNumFromQueryBuffer(index)
		if err != nil {
			return false, errors.Wrap(err, "failed to get bulk length")
		}

		client.moveToNextLineInQueryBuffer(index)

		if num == 0 {
			return true, nil
		}

		client.bulkLen = num
	}

	if client.queryLen < client.bulkLen+2 {
		return false, nil
	}

	client.args = append(client.args, datastruct.NewObject(datastruct.ObjectTypeString, string(client.queryBuf[0:client.bulkLen])))
	client.moveToNextLineInQueryBuffer(client.bulkLen)

	client.bulkLen = 0

	return true, nil
}

func processMultiBulkBuffer(client *Client) (bool, error) {
	if client.multiBulkLen == 0 {
		index := client.getCRLFIndexFromQueryBuffer()
		if index == -1 {
			if client.queryLen > MaxInlineSize {
				return false, errors.New("too big inline request")
			}

			return false, nil
		}

		num, err := client.getNumFromQueryBuffer(index)
		if err != nil {
			return false, errors.Wrap(err, "failed to get multi bulk length")
		}

		client.moveToNextLineInQueryBuffer(index)

		if num == 0 {
			return true, nil
		}

		client.multiBulkLen = num
		client.args = make([]*datastruct.Object, num)
	}

	for client.multiBulkLen > 0 {
		if client.bulkLen == 0 {
			index := client.getCRLFIndexFromQueryBuffer()
			if index == -1 {
				if client.queryLen > MaxInlineSize {
					return false, errors.New("too big inline request")
				}

				return false, nil
			}

			if client.queryBuf[0] != '$' {
				return false, errors.New("expected '$' for bulk length")
			}

			num, err := client.getNumFromQueryBuffer(index)
			if err != nil {
				return false, errors.Wrap(err, "failed to get multi bulk length")
			}

			client.bulkLen = num
			client.moveToNextLineInQueryBuffer(index)
		}

		if client.queryLen < client.bulkLen {
			return false, nil
		}

		index := client.bulkLen
		if client.queryBuf[index] != '\r' || client.queryBuf[index+1] != '\n' {
			return false, errors.New("expected CRLF for end of bulk string")
		}

		client.args[len(client.args)-client.multiBulkLen] = datastruct.NewObject(datastruct.ObjectTypeString, string(client.queryBuf[:index]))
		client.bulkLen = 0
		client.multiBulkLen--

		client.moveToNextLineInQueryBuffer(index)
	}

	return true, nil
}
