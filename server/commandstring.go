package server

import (
	"log"

	"github.com/IfanTsai/metis/datastruct"
)

func setCommand(client *Client) error {
	key, value := client.args[1], client.args[2]
	_ = client.db.Expire.Delete(key)
	client.db.Dict.Set(key, value)

	return client.addReplyOK()
}

func getCommand(client *Client) error {
	key := client.args[1]

	// check if key expired
	if _, err := expireIfNeeded(client.db, key); err != nil {
		return client.addReplyErrorf("expireIfNeeded error: %v", err)
	}

	value := client.db.Dict.Get(key)
	if value == nil {
		return client.addReplyNull()
	}

	valueStr, ok := value.(string)
	if !ok {
		return client.addReplyError("value is not a string")
	}

	return client.addReplyBulkString(valueStr)
}

func randomGetCommand(client *Client) error {
	var entry *datastruct.DictEntry
	for {
		entry = client.db.Dict.GetRandomKey()
		if entry == nil {
			return client.addReplyNull()
		}

		expired, err := expireIfNeeded(client.db, entry.Key.(string))
		if err != nil {
			log.Println("expireIfNeeded error:", err)
		}

		if !expired {
			break
		}
	}

	if entry == nil {
		return client.addReplyNull()
	}

	keyStr := entry.Key.(string)

	return client.addReplyBulkString(keyStr)
}
