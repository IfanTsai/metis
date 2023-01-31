package server

import (
	"strings"

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
	// TODO: implement more commands
}

func pingCommand(client *Client) {
	client.addReplyString("+PONG\r\n")
}

func getCommand(client *Client) {
	key := client.args[1]
	value := client.srv.db.Dict.Get(key)
	switch {
	case value == nil:
		client.addReplyString("$-1\r\n")
	case value.Type != datastruct.ObjectTypeString:
		client.addReplyString("-ERR value is not a string\r\n")
	default:
		valueStr := value.Value.(string)
		client.addReplyStringf("$%d\r\n%s\r\n", len(valueStr), valueStr)
	}
}

func setCommand(client *Client) {
	key, value := client.args[1], client.args[2]
	client.srv.db.Dict.Set(key, value)
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
	cmd := lookupCommand(strings.ToLower(client.args[0].Value.(string)))
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
