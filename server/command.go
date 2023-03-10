package server

import (
	"strings"
	"time"

	"github.com/IfanTsai/metis/database"
	"github.com/pkg/errors"
)

type command struct {
	name  string
	proc  func(client *Client) error
	arity int
}

var commandTable = []command{
	// other
	{"ping", pingCommand, 1},
	// key
	{"expire", expireCommand, 3},
	{"ttl", ttlCommand, 2},
	{"keys", keysCommand, 2},
	// string
	{"set", setCommand, -3},
	{"get", getCommand, 2},
	{"randomget", randomGetCommand, 1},
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
	// list
	{"lpush", lPushCommand, -3},
	{"rpush", rPushCommand, -3},
	{"lpop", lPopCommand, 2},
	{"rpop", rPopCommand, 2},
	{"llen", lLenCommand, 2},
	{"lindex", lIndexCommand, 3},
	{"lrange", lRangeCommand, -4},
	// TODO: implement more commands
}

func expireIfNeeded(db *database.Databse, key string) (bool, error) {
	expireObj := db.Expire.Find(key)
	if expireObj != nil {
		when, ok := expireObj.Value.(int64)
		if !ok {
			return false, errors.New("expire value is not an integer")
		}

		if when < time.Now().UnixMilli() {
			_ = db.Dict.Delete(key)
			_ = db.Expire.Delete(key)

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

	cmd := lookupCommand(strings.ToLower(client.args[0]))
	switch {
	case cmd == nil:
		err = client.addReplyError("unknown command")
	case (cmd.arity > 0 && len(client.args) != cmd.arity) || (len(client.args) < -cmd.arity):
		err = client.addReplyError("wrong number of arguments")
	default:
		err = cmd.proc(client)
	}

	client.reset()

	return err
}
