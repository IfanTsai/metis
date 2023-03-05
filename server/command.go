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
	proc  func(client *Client) error
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

func pingCommand(client *Client) error {
	return client.addReplyString("+PONG\r\n")
}

func zAddCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		value = datastruct.NewZset(&database.DictType{})
		dict.Set(key, value)
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyString("-ERR wrong type\r\n")
	}

	for i := 2; i < len(client.args); i += 2 {
		score, err := strconv.ParseFloat(client.args[i], 64)
		if err != nil {
			return client.addReplyStringf("-ERR invalid score: %s, error: %v\r\n", client.args[i], err)
		}

		member := client.args[i+1]
		zset.Add(score, member)
	}

	return client.addReplyString("+OK\r\n")
}

func zCardCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyString("$-1\r\n")
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyString("-ERR wrong type\r\n")
	}

	return client.addReplyStringf(":%d\r\n", zset.Size())
}

func zScoreCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyString("$-1\r\n")
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyString("-ERR wrong type\r\n")
	}

	member := client.args[2]
	element := zset.Get(member)
	if element == nil {
		return client.addReplyString("$-1\r\n")
	}

	scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)

	return client.addReplyStringf("+%s\r\n", scoreStr)
}

func zCountCommand(client *Client) error {
	key := client.args[1]
	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyString("$-1\r\n")
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyString("-ERR wrong type\r\n")
	}

	min, err := strconv.ParseFloat(client.args[2], 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid min: %s, error: %v\r\n", client.args[2], err)
	}

	max, err := strconv.ParseFloat(client.args[3], 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid max: %s, error: %v\r\n", client.args[3], err)
	}

	count := zset.Count(min, max)

	return client.addReplyStringf(":%d\r\n", count)
}

func zRangeCommand(client *Client) error {
	key := client.args[1]

	start, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid start: %s, error: %v\r\n", client.args[2], err)
	}

	stop, err := strconv.ParseInt(client.args[3], 10, 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid start: %s, error: %v\r\n", client.args[3], err)
	}

	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyString("$-1\r\n")
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyString("-ERR wrong type\r\n")
	}

	elements := zset.RangeByRank(start, stop, false)

	return client.addReplyZsetElements(elements, 4)
}

func zRangeByScoreCommand(client *Client) error {
	key := client.args[1]

	min, err := strconv.ParseFloat(client.args[2], 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid start: %s, error: %v\r\n", client.args[2], err)
	}

	max, err := strconv.ParseFloat(client.args[3], 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid start: %s, error: %v\r\n", client.args[3], err)
	}

	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyString("$-1\r\n")
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyString("-ERR wrong type\r\n")
	}

	elements := zset.RangeByScore(min, max, -1, false)

	return client.addReplyZsetElements(elements, 4)
}

func zRemCommand(client *Client) error {
	key := client.args[1]

	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyString("$-1\r\n")
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyString("-ERR wrong type\r\n")
	}

	for i := 2; i < len(client.args); i++ {
		zset.Delete(client.args[i])
	}

	return client.addReplyString("+OK\r\n")
}

func zRemRangeByRankCommand(client *Client) error {
	key := client.args[1]

	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyString("$-1\r\n")
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyString("-ERR wrong type\r\n")
	}

	start, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid start: %s, error: %v\r\n", client.args[2], err)
	}

	stop, err := strconv.ParseInt(client.args[3], 10, 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid start: %s, error: %v\r\n", client.args[3], err)
	}

	zset.DeleteRangeByRank(start, stop)

	return client.addReplyString("+OK\r\n")
}

func zRemRangeByScoreCommand(client *Client) error {
	key := client.args[1]

	dict := client.srv.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyString("$-1\r\n")
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyString("-ERR wrong type\r\n")
	}

	min, err := strconv.ParseFloat(client.args[2], 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid start: %s, error: %v\r\n", client.args[2], err)
	}

	max, err := strconv.ParseFloat(client.args[3], 64)
	if err != nil {
		return client.addReplyStringf("-ERR invalid start: %s, error: %v\r\n", client.args[3], err)
	}

	zset.DeleteRangeByScore(min, max)

	return client.addReplyString("+OK\r\n")
}

func keysCommand(client *Client) error {
	dict := client.srv.db.Dict
	iter := datastruct.NewDictIterator(dict)
	defer iter.Release()

	pattern := client.args[1]
	keys := make([]string, 0, dict.Size())
	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		key := entry.Key.(string)
		matched := false

		if pattern == "*" {
			matched = true
		} else {
			reg, err := regexp.Compile(pattern)
			if err != nil {
				return client.addReplyStringf("-ERR invalid pattern: %s, error: %v\r\n", pattern, err)
			}

			matched = reg.MatchString(key)
		}

		if matched {
			expired, err := expireIfNeeded(client.srv.db, entry.Key.(string))
			if err != nil {
				log.Println("expireIfNeeded error:", err)
			}

			if !expired {
				keys = append(keys, key)
			}
		}
	}

	if err := client.addReplyStringf("*%d\r\n", len(keys)); err != nil {
		return err
	}

	for _, key := range keys {
		if err := client.addReplyStringf("$%d\r\n%s\r\n", len(key), key); err != nil {
			return err
		}
	}

	return nil
}

func ttlCommand(client *Client) error {
	key := client.args[1]

	if client.srv.db.Dict.Find(key) == nil {
		return client.addReplyString(":-2\r\n")
	}

	expireEntry := client.srv.db.Expire.Find(key)
	if expireEntry == nil {
		return client.addReplyString(":-1\r\n")
	}

	when, ok := expireEntry.Value.(int64)
	if !ok {
		return client.addReplyString("-ERR expire value is not an integer\r\n")
	}

	ttl := (when - time.Now().UnixMilli()) / 1000

	return client.addReplyStringf(":%d\r\n", ttl)
}

func randomGetCommand(client *Client) error {
	var entry *datastruct.DictEntry
	for {
		entry = client.srv.db.Dict.GetRandomKey()
		if entry == nil {
			return client.addReplyString("$-1\r\n")
		}

		expired, err := expireIfNeeded(client.srv.db, entry.Key.(string))
		if err != nil {
			log.Println("expireIfNeeded error:", err)
		}

		if !expired {
			break
		}
	}

	if entry == nil {
		return client.addReplyString("$-1\r\n")
	}

	keyStr := entry.Key.(string)

	return client.addReplyStringf("$%d\r\n%s\r\n", len(keyStr), keyStr)
}

func getCommand(client *Client) error {
	key := client.args[1]

	// check if key expired
	if _, err := expireIfNeeded(client.srv.db, key); err != nil {
		return client.addReplyStringf("-ERR %v\r\n", err)
	}

	value := client.srv.db.Dict.Get(key)
	if value == nil {
		return client.addReplyString("$-1\r\n")
	}

	valueStr, ok := value.(string)
	if !ok {
		return client.addReplyString("-ERR value is not a string\r\n")
	}

	return client.addReplyStringf("$%d\r\n%s\r\n", len(valueStr), valueStr)
}

func setCommand(client *Client) error {
	key, value := client.args[1], client.args[2]
	_ = client.srv.db.Expire.Delete(key)
	client.srv.db.Dict.Set(key, value)

	return client.addReplyString("+OK\r\n")
}

func expireCommand(client *Client) error {
	key := client.args[1]

	expireInt, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		return client.addReplyStringf("-ERR value is not an integer, error: %v\r\n", err)
	}

	when := time.Now().UnixMilli() + expireInt*1000
	client.srv.db.Expire.Set(key, when)

	return client.addReplyString("+OK\r\n")
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
		err = client.addReplyString("-ERR unknown command\r\n")
	case (cmd.arity > 0 && len(client.args) != cmd.arity) || (len(client.args) < -cmd.arity):
		err = client.addReplyString("-ERR wrong number of arguments\r\n")
	default:
		err = cmd.proc(client)
	}

	client.reset()

	return err
}
