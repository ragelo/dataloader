package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync"
	"time"

	ex "github.com/ragelo/dataloader/examples"
)

func main() {
	// print server memory stats every 10 seconds
	wg := sync.WaitGroup{}
	wg.Add(4)

	go func() {
		ex.Start() // http://localhost:8080/users
		wg.Done()
	}()

	go func() {
		ex.StartGraphQL() // http://localhost:8081/graphql
		wg.Done()
	}()

	go func() {
		err := http.ListenAndServe("localhost:6060", nil) // http://localhost:6060/debug/pprof/heap
		if err != nil {
			panic(err)
		}
		wg.Done()
	}()

	go func() {
		var stats runtime.MemStats
		for {
			runtime.ReadMemStats(&stats)
			fmt.Printf("Alloc: %d, TotalAlloc: %d, Sys: %d, NumGC: %d, Goroutines: %d\n", stats.Alloc, stats.TotalAlloc, stats.Sys, stats.NumGC, runtime.NumGoroutine())
			time.Sleep(5 * time.Second)
		}
	}()
	wg.Wait()
}
