package server

import "strconv"

func pingCommand(client *Client) error {
	return client.addReplySimpleString("PONG")
}

func selectCommand(client *Client) error {
	dbIndex, err := strconv.Atoi(client.args[1])
	if err != nil {
		return client.addReplyError(err.Error())
	}

	if dbIndex >= len(client.srv.dbs) {
		return client.addReplyError("invalid db index")
	}

	client.db = client.srv.dbs[dbIndex]

	return client.addReplyOK()
}
