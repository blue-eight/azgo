package main

import (
	"log"
	"os"

	"github.com/blue-eight/azgo/alpha/cmd"
	"github.com/mitchellh/cli"
)

func main() {
	c := cli.NewCLI("app", "1.0.0")
	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
		"hello": func() (cli.Command, error) {
			return &cmd.Hello{}, nil
		},
	}

	exitStatus, err := c.Run()
	if err != nil {
		log.Println(err)
	}

	os.Exit(exitStatus)
}
