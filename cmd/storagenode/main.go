package main

import (
	"flag"
	"log"

	"github.com/nikhilghind/shard-store/internal/storagenode"
)

func main() {
	port := flag.Int("port", 9001, "Storage node HTTP port")
	coordinatorURL := flag.String("coordinator-url", "http://localhost:8080", "Coordinator base URL")
	dataDir := flag.String("data-dir", "./data", "Directory to store objects")
	nodeID := flag.String("node-id", "node-1", "Unique node identifier")
	flag.Parse()

	node := storagenode.NewStorageNode(*nodeID, *dataDir, *coordinatorURL, *port)
	log.Fatal(node.Start())
}
