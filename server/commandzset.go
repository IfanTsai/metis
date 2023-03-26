package server

import (
	"strconv"

	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/datastruct"
)

func zAddCommand(client *Client) error {
	key := client.args[1]
	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		value = datastruct.NewZset(&database.DictType{})
		dict.Set(key, value)
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyError("wrong type")
	}

	for i := 2; i < len(client.args); i += 2 {
		score, err := strconv.ParseFloat(client.args[i], 64)
		if err != nil {
			return client.addReplyErrorf("invalid score: %s, error: %v", client.args[i], err)
		}

		member := client.args[i+1]
		zset.Add(score, member)
	}

	return client.addReplyOK()
}

func zCardCommand(client *Client) error {
	key := client.args[1]
	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyNull()
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyError("wrong type")
	}

	return client.addReplyInt(zset.Size())
}

func zScoreCommand(client *Client) error {
	key := client.args[1]
	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyNull()
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyError("wrong type")
	}

	member := client.args[2]
	element := zset.Get(member)
	if element == nil {
		return client.addReplyNull()
	}

	scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)

	return client.addReplySimpleString(scoreStr)
}

func zCountCommand(client *Client) error {
	key := client.args[1]
	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyNull()
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyError("wrong type")
	}

	min, err := strconv.ParseFloat(client.args[2], 64)
	if err != nil {
		return client.addReplyErrorf("invalid min: %s, error: %v", client.args[2], err)
	}

	max, err := strconv.ParseFloat(client.args[3], 64)
	if err != nil {
		return client.addReplyErrorf("invalid max: %s, error: %v", client.args[3], err)
	}

	count := zset.Count(min, max)

	return client.addReplyInt(count)
}

func zRangeCommand(client *Client) error {
	key := client.args[1]

	start, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[2], err)
	}

	stop, err := strconv.ParseInt(client.args[3], 10, 64)
	if err != nil {
		return client.addReplyErrorf("invalid stop: %s, error: %v", client.args[3], err)
	}

	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyNull()
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyError("wrong type")
	}

	elements := zset.RangeByRank(start, stop, false)

	return client.addReplyZsetElements(elements, 4)
}

func zRangeByScoreCommand(client *Client) error {
	key := client.args[1]

	min, err := strconv.ParseFloat(client.args[2], 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[2], err)
	}

	max, err := strconv.ParseFloat(client.args[3], 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[3], err)
	}

	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyNull()
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyError("wrong type")
	}

	elements := zset.RangeByScore(min, max, -1, false)

	return client.addReplyZsetElements(elements, 4)
}

func zRemCommand(client *Client) error {
	key := client.args[1]

	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyNull()
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyError("wrong type")
	}

	for i := 2; i < len(client.args); i++ {
		zset.Delete(client.args[i])
	}

	return client.addReplyOK()
}

func zRemRangeByRankCommand(client *Client) error {
	key := client.args[1]

	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyNull()
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyError("wrong type")
	}

	start, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[2], err)
	}

	stop, err := strconv.ParseInt(client.args[3], 10, 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[3], err)
	}

	zset.DeleteRangeByRank(start, stop)

	return client.addReplyOK()
}

func zRemRangeByScoreCommand(client *Client) error {
	key := client.args[1]

	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return client.addReplyNull()
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return client.addReplyError("wrong type")
	}

	min, err := strconv.ParseFloat(client.args[2], 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[2], err)
	}

	max, err := strconv.ParseFloat(client.args[3], 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[3], err)
	}

	zset.DeleteRangeByScore(min, max)

	return client.addReplyOK()
}
