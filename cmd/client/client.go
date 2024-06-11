package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

var target = flag.String("target", "http://localhost:8090", "request target")

func main() {
	flag.Parse()
	client := new(http.Client)
	client.Timeout = 10 * time.Second
	endpoints := []string{"api/v1/some-data", "api/v1/some-data2", "api/v1/some-data3"}

	for range time.Tick(1 * time.Second) {
		for _, endpoint := range endpoints {
			resp, err := client.Get(fmt.Sprintf("%s/%s", *target, endpoint))
			if err == nil {
				log.Printf("response %d", resp.StatusCode)
			} else {
				log.Printf("error %s", err)
			}
		}
	}
}
