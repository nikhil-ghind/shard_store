package main

import (
	"flag"
	"log"

	"github.com/nikhilghind/shard-store/internal/coordinator"
)

func main() {
	port := flag.Int("port", 8080, "Coordinator HTTP port")
	flag.Parse()

	c := coordinator.NewCoordinator()
	log.Fatal(c.Start(*port))
}
