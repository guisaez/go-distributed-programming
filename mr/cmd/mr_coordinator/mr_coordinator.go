package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/guisaez/go-distributed-programming/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mr_coordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for !m.Done() {
		time.Sleep(time.Second)
	}

	log.Println("Coordinator exited!")

}
