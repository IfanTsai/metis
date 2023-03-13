package server

import (
	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/pkg/errors"
)

func sAddCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		value = datastruct.NewSet(&database.DictType{})
		dict.Set(key, value)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return client.addReplyError("wrong type")
	}

	for i := 2; i < len(client.args); i++ {
		set.Add(client.args[i])
	}

	return client.addReplyInt(set.Size())
}

func sRemCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyInt(0)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return client.addReplyError("wrong type")
	}

	deleted := 0
	for i := 2; i < len(client.args); i++ {
		if set.Delete(client.args[i]) == nil {
			deleted++
		}
	}

	return client.addReplyInt(set.Size())
}

func sPopCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyInt(0)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return client.addReplyError("wrong type")
	}

	if set.Size() == 0 {
		return client.addReplyNull()
	}

	randomMember := set.GetRandom()
	if err := set.Delete(randomMember); err != nil && !errors.Is(err, datastruct.ErrKeyNotFound) {
		return client.addReplyErrorf("delete random member error: %v", err)
	}

	return client.addReplyBulkString(randomMember.(string))
}

func sCardCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyInt(0)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return client.addReplyError("wrong type")
	}

	return client.addReplyInt(set.Size())
}

func sIsMemberCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyInt(0)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return client.addReplyError("wrong type")
	}

	if set.Contains(client.args[2]) {
		return client.addReplyInt(1)
	}

	return client.addReplyInt(0)
}

func sMembersCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyInt(0)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return client.addReplyError("wrong type")
	}

	return client.addReplySet(set)
}

func sDiffCommand(client *Client) error {
	dict := client.srv.db.Dict

	value := dict.Get(client.args[1])
	if value == nil {
		return client.addReplyInt(0)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return client.addReplyError("wrong type")
	}

	for i := 2; i < len(client.args); i++ {
		value := dict.Get(client.args[i])
		if value == nil {
			continue
		}

		set2, ok := value.(*datastruct.Set)
		if !ok {
			return client.addReplyError("wrong type")
		}

		set = set.Difference(set2)
	}

	return client.addReplySet(set)
}

func sInterCommand(client *Client) error {
	dict := client.srv.db.Dict

	value := dict.Get(client.args[1])
	if value == nil {
		return client.addReplyInt(0)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return client.addReplyError("wrong type")
	}

	for i := 2; i < len(client.args); i++ {
		value := dict.Get(client.args[i])
		if value == nil {
			continue
		}

		set2, ok := value.(*datastruct.Set)
		if !ok {
			return client.addReplyError("wrong type")
		}

		set = set.Intersect(set2)
	}

	return client.addReplySet(set)

}

func sUnionCommand(client *Client) error {
	dict := client.srv.db.Dict

	value := dict.Get(client.args[1])
	if value == nil {
		return client.addReplyInt(0)
	}

	set, ok := value.(*datastruct.Set)
	if !ok {
		return client.addReplyError("wrong type")
	}

	for i := 2; i < len(client.args); i++ {
		value := dict.Get(client.args[i])
		if value == nil {
			continue
		}

		set2, ok := value.(*datastruct.Set)
		if !ok {
			return client.addReplyError("wrong type")
		}

		set = set.Union(set2)
	}

	return client.addReplySet(set)
}
