package main

// To start a worker
//
// go run mr_worker.go *.so
//

import (
	"fmt"
	"os"
	"plugin"

	"github.com/guisaez/go-distributed-programming/mr"
	log "github.com/sirupsen/logrus"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mr_worker xxx.so\n")
		os.Exit(1)
	}

	log.Info("Loading plugin...")
	mapFun, reduceFun := loadPlugin(os.Args[1])

	log.Info("Initializing worker process")
	mr.Worker(mapFun, reduceFun)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
