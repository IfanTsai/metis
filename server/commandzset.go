package server

import (
	"strconv"

	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/pkg/errors"
)

func zAddCommand(client *Client) error {
	key := client.args[1]

	zset, err := getZset(client, key)
	if err != nil {
		return client.addReplyError(err.Error())
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

	zset, err := getZsetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	return client.addReplyInt(zset.Size())
}

func zScoreCommand(client *Client) error {
	key := client.args[1]

	zset, err := getZsetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyNull()
		}

		return client.addReplyError(err.Error())
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

	min, err := strconv.ParseFloat(client.args[2], 64)
	if err != nil {
		return client.addReplyErrorf("invalid min: %s, error: %v", client.args[2], err)
	}

	max, err := strconv.ParseFloat(client.args[3], 64)
	if err != nil {
		return client.addReplyErrorf("invalid max: %s, error: %v", client.args[3], err)
	}

	zset, err := getZsetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
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

	zset, err := getZsetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyNull()
		}

		return client.addReplyError(err.Error())
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

	zset, err := getZsetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyNull()
		}

		return client.addReplyError(err.Error())
	}

	elements := zset.RangeByScore(min, max, -1, false)

	return client.addReplyZsetElements(elements, 4)
}

func zRemCommand(client *Client) error {
	key := client.args[1]

	zset, err := getZsetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	var deleted int
	for i := 2; i < len(client.args); i++ {
		if zset.Delete(client.args[i]) {
			deleted++
		}
	}

	return client.addReplyInt(int64(deleted))
}

func zRemRangeByRankCommand(client *Client) error {
	key := client.args[1]

	start, err := strconv.ParseInt(client.args[2], 10, 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[2], err)
	}

	stop, err := strconv.ParseInt(client.args[3], 10, 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[3], err)
	}

	zset, err := getZsetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	deletedElements := zset.DeleteRangeByRank(start, stop)

	return client.addReplyInt(int64(len(deletedElements)))
}

func zRemRangeByScoreCommand(client *Client) error {
	key := client.args[1]

	min, err := strconv.ParseFloat(client.args[2], 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[2], err)
	}

	max, err := strconv.ParseFloat(client.args[3], 64)
	if err != nil {
		return client.addReplyErrorf("invalid start: %s, error: %v", client.args[3], err)
	}

	zset, err := getZsetIfExist(client, key)
	if err != nil {
		if errors.Is(err, errNotExist) {
			return client.addReplyInt(0)
		}

		return client.addReplyError(err.Error())
	}

	deletedElements := zset.DeleteRangeByScore(min, max)

	return client.addReplyInt(int64(len(deletedElements)))
}

func getZset(client *Client, key string) (*datastruct.Zset, error) {
	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		value = datastruct.NewZset(&database.DictType{})
		dict.Set(key, value)
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return nil, errWrongType
	}

	return zset, nil
}

func getZsetIfExist(client *Client, key string) (*datastruct.Zset, error) {
	// check if key expired
	if _, err := expireIfNeeded(client, key); err != nil {
		return nil, err
	}

	dict := client.db.Dict
	value := dict.Get(key)
	if value == nil {
		return nil, errNotExist
	}

	zset, ok := value.(*datastruct.Zset)
	if !ok {
		return nil, errWrongType
	}

	return zset, nil
}
