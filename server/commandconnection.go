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

func authCommand(client *Client) error {
	if client.srv.requirePassword == "" || client.srv.requirePassword == client.args[1] {
		client.authenticated = true
		return client.addReplyOK()
	}

	client.authenticated = false

	return client.addReplyError("invalid password")
}
