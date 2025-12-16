package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/pkg/errors"
)

var _ = net.Listen
var _ = os.Exit

func main() {
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", errors.New(err.Error()))
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
			break
		}
		fmt.Printf("Received: %s\n", string(buf[:n]))
		conn.Write([]byte("+PONG\r\n"))
	}
}
