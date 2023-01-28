package server

import (
	"fmt"
	"log"
)

// TODO: implement processCommand
func processCommand(client *Client) {
	fmt.Println("-----------------------")
	for _, arg := range client.args {
		fmt.Printf("%v\n", arg.Value)
	}
	fmt.Println("-----------------------")

	if err := client.addReplyString("+OK\r\n"); err != nil {
		log.Printf("addReply error: %v", err)
	}
}
