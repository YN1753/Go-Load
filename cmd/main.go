package main

import (
	"fmt"
	"go-load/pkg/engine"
)

func main() {

	fmt.Println("hello world")
	re := engine.Requester{
		URL:           "http://127.0.0.1:8181",
		Concurrency:   100,
		TotalRequests: 1000000,
	}
	re.Run()
}
