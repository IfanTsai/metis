package server

import (
	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/pkg/errors"
)

func sAddCommand(client *Client) error {
	key := client.args[1]

	set, err := getSet(client, key)
	if err != nil {
		return client.addReplyError(err.Error())
	}

	var created int64
	for i := 2; i < len(client.args); i++ {
		if set.Add(client.args[i]) {
			created++
		}

		client.srv.dirty++
	}

	return client.addReplyInt(created)
}

func sRemCommand(client *Client) error {
	key := client.args[1]

	set, err := getSetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	var deleted int64
	for i := 2; i < len(client.args); i++ {
		if set.Delete(client.args[i]) == nil {
			deleted++
			client.srv.dirty++
		}
	}

	return client.addReplyInt(deleted)
}

func sPopCommand(client *Client) error {
	key := client.args[1]

	set, err := getSetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyNull()
		}

		return client.addReplyError(err.Error())
	}

	if set.Size() == 0 {
		return client.addReplyNull()
	}

	randomMember := set.GetRandom()
	if err := set.Delete(randomMember); err != nil && !errors.Is(err, datastruct.ErrKeyNotFound) {
		return client.addReplyErrorf("delete random member error: %v", err)
	}

	client.srv.dirty++

	return client.addReplyBulkString(randomMember.(string))
}

func sCardCommand(client *Client) error {
	key := client.args[1]

	set, err := getSetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	return client.addReplyInt(set.Size())
}

func sIsMemberCommand(client *Client) error {
	key := client.args[1]

	set, err := getSetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	if set.Contains(client.args[2]) {
		return client.addReplyInt(1)
	}

	return client.addReplyInt(0)
}

func sMembersCommand(client *Client) error {
	key := client.args[1]

	set, err := getSetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyEmpty()
		}

		return client.addReplyError(err.Error())
	}

	return client.addReplySet(set)
}

func sDiffCommand(client *Client) error {
	set, err := getSetIfExist(client, client.args[1])
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyEmpty()
		}

		return client.addReplyError(err.Error())
	}

	for i := 2; i < len(client.args); i++ {
		set2, err := getSetIfExist(client, client.args[i])
		if err != nil {
			if errors.Is(err, errNotExist) {
				continue
			}

			return client.addReplyError(err.Error())
		}

		set = set.Difference(set2)
	}

	return client.addReplySet(set)
}

func sInterCommand(client *Client) error {
	set, err := getSetIfExist(client, client.args[1])
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyEmpty()
		}

		return client.addReplyError(err.Error())
	}

	for i := 2; i < len(client.args); i++ {
		set2, err := getSetIfExist(client, client.args[i])
		if err != nil {
			if errors.Is(err, errNotExist) {
				return client.addReplyEmpty()
			}

			return client.addReplyError(err.Error())
		}

		set = set.Intersect(set2)
	}

	return client.addReplySet(set)
}

func sUnionCommand(client *Client) error {
	set, err := getSetIfExist(client, client.args[1])
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyEmpty()
		}

		return client.addReplyError(err.Error())
	}

	for i := 2; i < len(client.args); i++ {
		set2, err := getSetIfExist(client, client.args[i])
		if err != nil {
			if errors.Is(err, errNotExist) {
				continue
			}

			return client.addReplyError(err.Error())
		}

		set = set.Union(set2)
	}

	return client.addReplySet(set)
}

func getSet(client *Client, key string) (*datastruct.Set, error) {
	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		value = datastruct.NewSet(&database.DictType{})
		dict.Set(key, value)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return nil, errWrongType
	}

	return set, nil
}

func getSetIfExist(client *Client, key string) (*datastruct.Set, error) {
	// check if key expired
	if _, err := expireIfNeeded(client, key); err != nil {
		return nil, err
	}

	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return nil, errNotExist
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return nil, errWrongType
	}

	return set, nil
}
