package main

//
// To start the coordinator process
//
// go run mr_coordinator.go pg*.txt
//
import (
	"fmt"
	"os"
	"time"

	"github.com/guisaez/go-distributed-programming/mr"
	log "github.com/sirupsen/logrus"
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

	log.Info("Coordinator exited!")
}
