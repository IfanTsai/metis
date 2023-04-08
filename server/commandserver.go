package server

func bgRewriteAofCommand(client *Client) error {
	srv := client.srv

	if srv.backgroundTaskTypeAtomic.Load() != uint32(TypeBackgroundTaskNone) {
		return client.addReplySimpleString("Background append only file rewriting scheduled")
	}

	rewriteAppendOnlyFileBackground(srv)

	return client.addReplySimpleString("Background append only file rewriting started")
}
