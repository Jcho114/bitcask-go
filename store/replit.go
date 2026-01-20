package store

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func (s *store) RunReplit() error {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		line := scanner.Text()
		if err := s.handleLine(line); err != nil {
			return fmt.Errorf("replit: %v", err)
		}
		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("replit: %v", err)
	}

	return nil
}

func (s *store) handleLine(line string) error {
	split := strings.Split(line, " ")
	command := split[0]
	switch command {
	case "GET":
		if len(split) != 2 {
			fmt.Println("INVALID COMMAND: GET REQUIRES 1 ARGUMENT")
			return nil
		}
		key := split[1]
		value, err := s.get(key)
		if err != nil {
			return fmt.Errorf("handle: %v", err)
		}
		if value == nil {
			fmt.Println("NULL")
		} else {
			fmt.Println(string(value))
		}
	case "PUT":
		if len(split) != 3 {
			fmt.Println("INVALID COMMAND: PUT REQUIRES 2 ARGUMENTS")
			return nil
		}
		key, value := split[1], []byte(split[2])
		err := s.put(key, value)
		if err != nil {
			return fmt.Errorf("handle: %v", err)
		}
		fmt.Printf("PUT %s SUCCESSFULLY\n", split[1])
	case "DELETE":
		if len(split) != 2 {
			fmt.Println("INVALID COMMAND: DELETE REQUIRES 1 ARGUMENT")
			return nil
		}
		key := split[1]
		err := s.delete(key)
		if err != nil {
			return fmt.Errorf("handle: %v", err)
		}
		fmt.Printf("DELETE %s SUCCESSFULLY\n", split[1])
	case "KEYS":
		if len(split) != 1 {
			fmt.Println("INVALID COMMAND: KEYS REQUIRES NO ARGUMENTS")
			return nil
		}
		keys := s.keys()
		if len(keys) == 0 {
			fmt.Println("NO KEYS")
		} else {
			fmt.Println(strings.Join(keys, ", "))
		}
	default:
		fmt.Printf("INVALID COMMAND: %s IS NOT A COMMAND\n", command)
	}
	return nil
}
