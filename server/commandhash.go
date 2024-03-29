package server

import (
	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/pkg/errors"
)

func hSetCommand(client *Client) error {
	if len(client.args)&1 != 0 {
		return client.addReplyError("wrong number of arguments for 'hset' command")
	}

	key := client.args[1]

	hash, err := getHash(client, key)
	if err != nil {
		return client.addReplyError(err.Error())
	}

	var created int64
	for i := 2; i < len(client.args); i += 2 {
		if hash.Set(client.args[i], client.args[i+1]) {
			created++
		}
		client.srv.dirty++
	}

	return client.addReplyInt(created)
}

func hGetCommand(client *Client) error {
	key, field := client.args[1], client.args[2]

	hash, err := getHashIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyNull()
		}

		return client.addReplyError(err.Error())
	}

	fieldValue := hash.Get(field)
	if fieldValue == nil {
		return client.addReplyNull()
	}

	return client.addReplyBulkString(fieldValue.(string))
}

func hDelCommand(client *Client) error {
	key := client.args[1]

	hash, err := getHashIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	var deleted int64
	for _, field := range client.args[2:] {
		if hash.Delete(field) == nil {
			deleted++
			client.srv.dirty++
		}
	}

	return client.addReplyInt(deleted)
}

func hExistsCommand(client *Client) error {
	key, field := client.args[1], client.args[2]

	hash, err := getHashIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	if hash.Find(field) == nil {
		return client.addReplyInt(0)
	}

	return client.addReplyInt(1)
}

func hKeysCommand(client *Client) error {
	key := client.args[1]

	hash, err := getHashIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyEmpty()
		}

		return client.addReplyError(err.Error())
	}

	iter := datastruct.NewDictIterator(hash)
	defer iter.Release()

	keys := make([]string, 0, hash.Size())
	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		keys = append(keys, entry.Key.(string))
	}

	return client.addReplyArrays(keys)
}

func hLenCommand(client *Client) error {
	key := client.args[1]

	hash, err := getHashIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	return client.addReplyInt(hash.Size())
}

func getHash(client *Client, key string) (*datastruct.Dict, error) {
	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		value = datastruct.NewDict(&database.DictType{})
		dict.Set(key, value)
	}

	hash, ok := value.(*datastruct.Dict)
	if !ok {
		return nil, errWrongType
	}

	return hash, nil
}

func getHashIfExist(client *Client, key string) (*datastruct.Dict, error) {
	// check if key expired
	if _, err := expireIfNeeded(client, key); err != nil {
		return nil, err
	}

	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return nil, errNotExist
	}

	hash, ok := value.(*datastruct.Dict)
	if !ok {
		return nil, errWrongType
	}

	return hash, nil
}
