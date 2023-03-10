package server

import (
	"log"
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
	client.srv.db.Expire.Set(key, when)

	return client.addReplyOK()
}

func ttlCommand(client *Client) error {
	key := client.args[1]

	if client.srv.db.Dict.Find(key) == nil {
		return client.addReplyInt(-2)
	}

	expireEntry := client.srv.db.Expire.Find(key)
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
	dict := client.srv.db.Dict
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
			expired, err := expireIfNeeded(client.srv.db, entry.Key.(string))
			if err != nil {
				log.Println("expireIfNeeded error:", err)
			}

			if !expired {
				keys = append(keys, key)
			}
		}
	}

	return client.addReplyArrays(keys)
}
