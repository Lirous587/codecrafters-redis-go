package main

import (
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment the code below to pass the first stage

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	conn.Write([]byte("+PONG\r\n"))

	// for {
	// 	buf := make([]byte, 1024)
	// 	n, err := conn.Read(buf)
	// 	if err != nil {
	// 		fmt.Println("Error reading:", err.Error())
	// 		break
	// 	}
	// 	fmt.Printf("Received: %s\n", string(buf[:n]))
	// 	conn.Write([]byte("+PONG\r\n"))
	// }
}
