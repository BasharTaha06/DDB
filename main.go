package main

import (
	"ddb/server"
	"flag"
	"strings"
)

func main() {
	id := flag.String("id", "1", "Node ID")
	port := flag.String("port", "8081", "Node Port")
	peersFlag := flag.String("peers", "", "Comma separated list of peer URLs")
	flag.Parse()

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	node := server.NewNode(*id, *port, peers)
	node.Run()
}
