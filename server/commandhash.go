package server

import (
	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/datastruct"
)

func hSetCommand(client *Client) error {
	key, field, fieldValue := client.args[1], client.args[2], client.args[3]

	hash, err := getHash(client, key)
	if hash == nil {
		return err
	}

	hash.Set(field, fieldValue)

	return client.addReplyInt(1)
}

func hGetCommand(client *Client) error {
	key, field := client.args[1], client.args[2]

	hash, err := getHashIfExist(client, key)
	if hash == nil {
		return err
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
	if hash == nil {
		return err
	}

	var deleted int
	for _, field := range client.args[2:] {
		if hash.Delete(field) == nil {
			deleted++
		}
	}

	return client.addReplyInt(int64(deleted))
}

func hExistsCommand(client *Client) error {
	key, field := client.args[1], client.args[2]

	hash, err := getHashIfExist(client, key)
	if hash == nil {
		return err
	}

	if hash.Find(field) == nil {
		return client.addReplyInt(0)
	}

	return client.addReplyInt(1)
}

func hKeysCommand(client *Client) error {
	key := client.args[1]

	hash, err := getHashIfExist(client, key)
	if hash == nil {
		return err
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
	if hash == nil {
		return err
	}

	return client.addReplyInt(hash.Size())
}

func getHash(client *Client, key string) (*datastruct.Dict, error) {
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		value = datastruct.NewDict(&database.DictType{})
		dict.Set(key, value)
	}

	hash, ok := value.(*datastruct.Dict)
	if !ok {
		return nil, client.addReplyError("wrong type")
	}

	return hash, nil
}

func getHashIfExist(client *Client, key string) (*datastruct.Dict, error) {
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return nil, client.addReplyInt(0)
	}

	hash, ok := value.(*datastruct.Dict)
	if !ok {
		return nil, client.addReplyError("wrong type")
	}

	return hash, nil
}
