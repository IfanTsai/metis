package server

import (
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/pkg/errors"
)

type command struct {
	name  string
	proc  func(client *Client)
	arity int
}

var commandTable = []command{
	{"ping", pingCommand, 1},
	{"get", getCommand, 2},
	{"set", setCommand, -3},
	{"expire", expireCommand, 3},
	{"randomget", randomGetCommand, 1},
	{"ttl", ttlCommand, 2},
	{"keys", keysCommand, 2},
	// TODO: implement more commands
}

func pingCommand(client *Client) {
	client.addReplyString("+PONG\r\n")
}

func keysCommand(client *Client) {
	dict := client.srv.db.Dict
	iter := datastruct.NewDictIterator(dict)
	defer iter.Release()

	pattern := client.args[1].StrValue()
	keys := make([]string, 0, dict.Size())
	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		key := entry.Key.StrValue()
		matched := false

		if pattern == "*" {
			matched = true
		} else {
			reg, err := regexp.Compile(pattern)
			if err != nil {
				client.addReplyStringf("-ERR invalid pattern: %s, error: %v\r\n", pattern, err)
				return
			}

			matched = reg.MatchString(key)
		}

		if matched {
			expired, err := expireIfNeeded(client.srv.db, entry.Key)
			if err != nil {
				log.Println("expireIfNeeded error:", err)
			}

			if !expired {
				keys = append(keys, key)
			}
		}
	}

	client.addReplyStringf("*%d\r\n", len(keys))
	for _, key := range keys {
		client.addReplyStringf("$%d\r\n%s\r\n", len(key), key)
	}
}

func ttlCommand(client *Client) {
	key := client.args[1]

	if client.srv.db.Dict.Find(key) == nil {
		client.addReplyString(":-2\r\n")

		return
	}

	expireObj := client.srv.db.Expire.Find(key)
	if expireObj == nil {
		client.addReplyString(":-1\r\n")

		return
	}

	when, err := expireObj.Value.IntValue()
	if err != nil {
		client.addReplyStringf("-ERR expire value is not an intege, error: %v\r\n", err)

		return
	}

	ttl := (when - time.Now().UnixMilli()) / 1000
	client.addReplyStringf(":%d\r\n", ttl)
}

func randomGetCommand(client *Client) {
	var entry *datastruct.DictEntry
	for {
		entry = client.srv.db.Dict.GetRandomKey()
		if entry == nil {
			client.addReplyString("$-1\r\n")

			return
		}

		expired, err := expireIfNeeded(client.srv.db, entry.Key)
		if err != nil {
			log.Println("expireIfNeeded error:", err)
		}

		if !expired {
			break
		}
	}

	if entry == nil {
		client.addReplyString("$-1\r\n")

		return
	}

	switch {
	case entry == nil:
		client.addReplyString("$-1\r\n")
	case entry.Key.Type != datastruct.ObjectTypeString:
		client.addReplyString("-ERR key is not a string\r\n")
	default:
		keyStr := entry.Key.StrValue()
		client.addReplyStringf("$%d\r\n%s\r\n", len(keyStr), keyStr)
	}
}

func getCommand(client *Client) {
	key := client.args[1]

	// check if key expired
	if _, err := expireIfNeeded(client.srv.db, key); err != nil {
		client.addReplyStringf("-ERR %v\r\n", err)
	}

	value := client.srv.db.Dict.Get(key)
	switch {
	case value == nil:
		client.addReplyString("$-1\r\n")
	case value.Type != datastruct.ObjectTypeString:
		client.addReplyString("-ERR value is not a string\r\n")
	default:
		valueStr := value.StrValue()
		client.addReplyStringf("$%d\r\n%s\r\n", len(valueStr), valueStr)
	}
}

func setCommand(client *Client) {
	key, value := client.args[1], client.args[2]
	client.srv.db.Expire.Delete(key)
	client.srv.db.Dict.Set(key, value)
	client.addReplyString("+OK\r\n")
}

func expireCommand(client *Client) {
	expireInt, err := client.args[2].IntValue()
	if err != nil {
		client.addReplyStringf("-ERR value is not an integer, error: %v\r\n", err)

		return
	}

	when := time.Now().UnixMilli() + expireInt*1000

	expireObj := datastruct.NewObject(
		datastruct.ObjectTypeString,
		strconv.FormatInt(when, 10),
	)

	client.srv.db.Expire.Set(client.args[1], expireObj)
	client.addReplyString("+OK\r\n")
}

func expireIfNeeded(db *database.Databse, key *datastruct.Object) (bool, error) {
	expireObj := db.Expire.Find(key)
	if expireObj != nil {
		when, err := expireObj.Value.IntValue()
		if err != nil {
			return false, errors.Wrap(err, "expire value is not an integer")
		}

		if when < time.Now().UnixMilli() {
			db.Dict.Delete(key)
			db.Expire.Delete(key)

			return true, nil
		}
	}

	return false, nil
}

func lookupCommand(name string) *command {
	for _, cmd := range commandTable {
		if cmd.name == name {
			return &cmd
		}
	}
	return nil
}

func processCommand(client *Client) {
	cmd := lookupCommand(strings.ToLower(client.args[0].StrValue()))
	switch {
	case cmd == nil:
		client.addReplyString("-ERR unknown command\r\n")
	case (cmd.arity > 0 && len(client.args) != cmd.arity) || (len(client.args) < -cmd.arity):
		client.addReplyString("-ERR wrong number of arguments\r\n")
	default:
		cmd.proc(client)
	}

	client.reset()
}
