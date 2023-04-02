package server

import (
	"strconv"

	"github.com/IfanTsai/metis/datastruct"
	"github.com/pkg/errors"
)

func lPushCommand(client *Client) error {
	key := client.args[1]

	list, err := getQuickList(client, key)
	if err != nil {
		return client.addReplyError(err.Error())
	}

	for i := 2; i < len(client.args); i++ {
		list.PushFront(client.args[i])
		client.srv.dirty++
	}

	return client.addReplyInt(int64(list.Len()))
}

func rPushCommand(client *Client) error {
	key := client.args[1]

	list, err := getQuickList(client, key)
	if err != nil {
		return client.addReplyError(err.Error())
	}

	for i := 2; i < len(client.args); i++ {
		list.PushBack(client.args[i])
		client.srv.dirty++
	}

	return client.addReplyInt(int64(list.Len()))
}

func lPopCommand(client *Client) error {
	key := client.args[1]

	list, err := getQuickListIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyNull()
		}

		return client.addReplyError(err.Error())
	}

	if list.Len() == 0 {
		return client.addReplyNull()
	}

	client.srv.dirty++

	return client.addReplyBulkString(list.PopFront().(string))
}

func rPopCommand(client *Client) error {
	key := client.args[1]

	list, err := getQuickListIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyNull()
		}

		return client.addReplyError(err.Error())
	}

	if list.Len() == 0 {
		return client.addReplyNull()
	}

	client.srv.dirty++

	return client.addReplyBulkString(list.PopBack().(string))
}

func lLenCommand(client *Client) error {
	key := client.args[1]

	list, err := getQuickListIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	return client.addReplyInt(int64(list.Len()))
}

func lIndexCommand(client *Client) error {
	key := client.args[1]

	list, err := getQuickListIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyNull()
		}

		return client.addReplyError(err.Error())
	}

	index, err := strconv.Atoi(client.args[2])
	if err != nil {
		return client.addReplyErrorf("invalid index: %s, error: %v", client.args[2], err)
	}

	v := list.Get(index)
	if v == nil {
		return client.addReplyNull()
	}

	return client.addReplyBulkString(v.(string))
}

func lRangeCommand(client *Client) error {
	key := client.args[1]

	list, err := getQuickListIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyEmpty()
		}

		return client.addReplyError(err.Error())
	}

	start, err := strconv.Atoi(client.args[2])
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[2], err)
	}

	stop, err := strconv.Atoi(client.args[3])
	if err != nil {
		return client.addReplyErrorf("invalid stop: %s, error: %v", client.args[3], err)
	}

	values := list.Range(start, stop)
	if values == nil {
		return client.addReplyNull()
	}

	strs := make([]string, len(values))
	for i, v := range values {
		strs[i] = v.(string)
	}

	return client.addReplyArrays(strs)
}

func getQuickList(client *Client, key string) (*datastruct.Quicklist, error) {
	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		value = datastruct.NewQuicklist()
		dict.Set(key, value)
	}

	list, ok := value.(*datastruct.Quicklist)
	if !ok {
		return nil, errNotExist
	}

	return list, nil
}

func getQuickListIfExist(client *Client, key string) (*datastruct.Quicklist, error) {
	// check if key expired
	if _, err := expireIfNeeded(client, key); err != nil {
		return nil, err
	}

	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return nil, errNotExist
	}

	list, ok := value.(*datastruct.Quicklist)
	if !ok {
		return nil, errWrongType
	}

	return list, nil
}
