package cmd

import "log"

type Hello struct{}

func (h *Hello) Run(args []string) int {
	log.Printf("hello\n")
	return 1
}

func (h *Hello) Help() string {
	return "[help]"
}

func (h *Hello) Synopsis() string {
	return "[synposis]"
}
