package main

import (
	"context"
	"log"
	"os/signal"
	"skiffdb/src/core"
	"skiffdb/src/server"
	"sync"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	core.InitDBConfig()

	var wg sync.WaitGroup

	server.InitRaft(ctx, &wg)
	server.InitRESPTCPServer(ctx, &wg)

	wg.Wait()
	log.Println("all servers stopped")
}
