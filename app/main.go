package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/connection"
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

	log.Println("server start at ", l.Addr())

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", errors.New(err.Error()))
		}

		go connection.Handle(conn)
	}
}
