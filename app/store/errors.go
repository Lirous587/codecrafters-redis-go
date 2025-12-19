package store

import "fmt"

func emsgArgsNumber(cmd string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd)
}

func emsgKeyType() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}
