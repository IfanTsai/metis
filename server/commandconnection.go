package server

func pingCommand(client *Client) error {
	return client.addReplySimpleString("PONG")
}
