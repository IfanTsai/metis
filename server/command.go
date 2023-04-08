package server

import (
	"strings"
	"time"

	"github.com/pkg/errors"
)

var (
	errNotExist  = errors.New("not exist")
	errWrongType = errors.New("wrong type")
)

type command struct {
	name  string
	proc  func(client *Client) error
	arity int
}

var commandTable = []command{
	// connection
	{"ping", pingCommand, 1},
	{"select", selectCommand, 2},
	{"auth", authCommand, 2},
	// server
	{"bgrewriteaof", bgRewriteAofCommand, 1},
	// key
	{"expire", expireCommand, 3},
	{"expireat", expireAtCommand, 3},
	{"ttl", ttlCommand, 2},
	{"keys", keysCommand, 2},
	// string
	{"set", setCommand, -3},
	{"setex", setExCommand, 4},
	{"get", getCommand, 2},
	{"randomget", randomGetCommand, 1},
	// hash
	{"hset", hSetCommand, -4},
	{"hget", hGetCommand, 3},
	{"hdel", hDelCommand, -3},
	{"hexists", hExistsCommand, 3},
	{"hkeys", hKeysCommand, 2},
	{"hlen", hLenCommand, 2},
	// list
	{"lpush", lPushCommand, -3},
	{"rpush", rPushCommand, -3},
	{"lpop", lPopCommand, 2},
	{"rpop", rPopCommand, 2},
	{"llen", lLenCommand, 2},
	{"lindex", lIndexCommand, 3},
	{"lrange", lRangeCommand, -4},
	// set
	{"sadd", sAddCommand, -3},
	{"srem", sRemCommand, -3},
	{"spop", sPopCommand, 2},
	{"scard", sCardCommand, 2},
	{"sismember", sIsMemberCommand, 3},
	{"smembers", sMembersCommand, 2},
	{"sdiff", sDiffCommand, -3},
	{"sinter", sInterCommand, -3},
	{"sunion", sUnionCommand, -3},
	// zset
	{"zadd", zAddCommand, -4},
	{"zrange", zRangeCommand, -4},
	{"zrangebyscore", zRangeByScoreCommand, -4},
	{"zrem", zRemCommand, -3},
	{"zremrangebyrank", zRemRangeByRankCommand, -4},
	{"zremrangebyscore", zRemRangeByScoreCommand, -4},
	{"zcard", zCardCommand, 2},
	{"zcount", zCountCommand, 4},
	{"zscore", zScoreCommand, 3},
	// TODO: implement more commands
}

func expireIfNeeded(client *Client, key string) (bool, error) {
	expireObj := client.db.Expire.Find(key)
	if expireObj != nil {
		when, ok := expireObj.Value.(int64)
		if !ok {
			return false, errors.New("expire value is not an integer")
		}

		if when < time.Now().UnixMilli() {
			_ = client.db.Dict.Delete(key)
			_ = client.db.Expire.Delete(key)

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

func processCommand(client *Client) error {
	var err error

	cmdName := strings.ToLower(client.args[0])
	cmd := lookupCommand(cmdName)
	switch {
	case cmd == nil:
		err = client.addReplyError("unknown command")
	case (cmd.arity > 0 && len(client.args) != cmd.arity) || (len(client.args) < -cmd.arity):
		err = client.addReplyError("wrong number of arguments")
	default:
		if client.srv.requirePassword != "" && !client.authenticated && cmdName != "auth" {
			err = client.addReplyError("operation not permitted")
			break
		}

		err = call(client, cmd)
	}

	client.reset()

	return err
}

// call is the core of Metis execution of a command.
func call(client *Client, cmd *command) error {
	dirty := client.srv.dirty
	if err := cmd.proc(client); err != nil {
		return err
	}
	dirty = client.srv.dirty - dirty

	if client.srv.aofEnable && dirty != 0 {
		feedAppendOnlyFile(client.srv, cmd, client.db.ID, client.args)
	}

	return nil
}
