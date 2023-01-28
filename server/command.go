package server

import "fmt"

// TODO: implement processCommand
func processCommand(client *Client) {
	fmt.Println("-----------------------")
	for _, arg := range client.args {
		fmt.Printf("%v\n", arg.Value)
	}
	fmt.Println("-----------------------")
	client.fd.Write([]byte("+OK\r\n"))
}
