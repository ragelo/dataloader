package main

import (
	"fmt"
	"runtime"
	"time"

	ex "github.com/ragelo/dataloader/examples"
)

func main() {
	// print server memory stats every 10 seconds
	go func() {
		for {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			fmt.Printf("Alloc: %d, TotalAlloc: %d, Sys: %d, NumGC: %d\n", stats.Alloc, stats.TotalAlloc, stats.Sys, stats.NumGC)
			fmt.Printf("Number of Goroutines: %d\n", runtime.NumGoroutine())
			time.Sleep(10 * time.Second)
		}
	}()

	ex.Start()
}
