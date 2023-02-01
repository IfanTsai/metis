package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/IfanTsai/metis/datastruct"
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
	// TODO: implement more commands
}

func pingCommand(client *Client) {
	client.addReplyString("+PONG\r\n")
}

func randomGetCommand(client *Client) {
	entry := client.srv.db.Dict.GetRandomKey()
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
	expireObj := client.srv.db.Expire.Find(key)
	if expireObj != nil {
		when, err := expireObj.Value.IntValue()
		if err != nil {
			client.addReplyStringf("-ERR expire value is not an intege, error: %v\r\n", err)

			return
		}

		if when < time.Now().UnixMilli() {
			client.srv.db.Dict.Delete(key)
			client.srv.db.Expire.Delete(key)
		}
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
