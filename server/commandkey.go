package server

import (
	"regexp"
	"strconv"
	"time"

	"github.com/IfanTsai/metis/datastruct"
)

func expireCommand(client *Client) error {
	key := client.args[1]

	expireInt, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		return client.addReplyErrorf("invalid expire: %s, error: %v", client.args[2], err)
	}

	when := time.Now().UnixMilli() + expireInt*1000
	client.db.Expire.Set(key, when)
	client.srv.dirty++

	return client.addReplyOK()
}

func expireAtCommand(client *Client) error {
	key := client.args[1]

	expireInt, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		return client.addReplyErrorf("invalid expire: %s, error: %v", client.args[2], err)
	}

	client.db.Expire.Set(key, expireInt*1000)
	client.srv.dirty++

	return client.addReplyOK()
}

func ttlCommand(client *Client) error {
	key := client.args[1]

	if client.db.Dict.Find(key) == nil {
		return client.addReplyInt(-2)
	}

	expireEntry := client.db.Expire.Find(key)
	if expireEntry == nil {
		return client.addReplyInt(-1)
	}

	when, ok := expireEntry.Value.(int64)
	if !ok {
		return client.addReplyError("expire value is not an integer")
	}

	ttl := (when - time.Now().UnixMilli()) / 1000

	return client.addReplyInt(ttl)
}

func keysCommand(client *Client) error {
	dict := client.db.Dict
	iter := datastruct.NewDictIterator(dict)
	defer iter.Release()

	pattern := client.args[1]
	keys := make([]string, 0, dict.Size())
	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		key := entry.Key.(string)
		matched := false

		if pattern == "*" {
			matched = true
		} else {
			reg, err := regexp.Compile(pattern)
			if err != nil {
				return client.addReplyErrorf("invalid pattern: %s, error: %v", pattern, err)
			}

			matched = reg.MatchString(key)
		}

		if matched {
			expired, err := expireIfNeeded(client, entry.Key.(string))
			if err != nil {
				return client.addReplyError(err.Error())
			}

			if !expired {
				keys = append(keys, key)
			}
		}
	}

	return client.addReplyArrays(keys)
}
